#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

/* Native fixture for the exact backend CLI/lifetime surface owned by the pair. */
static volatile sig_atomic_t stopping;
static char api_socket[sizeof(((struct sockaddr_un *)0)->sun_path)];
static char client_socket[sizeof(((struct sockaddr_un *)0)->sun_path)];
static char pid_path[4096];
static int api_fd = -1, client_fd = -1;

static void on_signal(int signal_number) { (void)signal_number; stopping = 1; }

static int join(char *out, size_t n, const char *a, const char *b) {
  int written = snprintf(out, n, "%s/%s", a, b);
  return written >= 0 && (size_t)written < n ? 0 : -1;
}

static int mkdirs(const char *path) {
  char copy[4096]; size_t n = strlen(path);
  if (!n || n >= sizeof copy) return -1;
  memcpy(copy, path, n + 1);
  for (char *p = copy + 1; *p; ++p) {
    if (*p != '/') continue;
    *p = '\0';
    if (mkdir(copy, 0700) < 0 && errno != EEXIST) return -1;
    *p = '/';
  }
  return mkdir(copy, 0700) == 0 || errno == EEXIST ? 0 : -1;
}

static int has(const char *state, const char *name) {
  char path[4096];
  return join(path, sizeof path, state, name) == 0 && access(path, F_OK) == 0;
}

static int paths(const char *session, const char *config, const char *state) {
  char sessions[4096], directory[4096], leaf[512];
  if (join(sessions, sizeof sessions, config, "herdr/sessions") < 0 ||
      join(directory, sizeof directory, sessions, session) < 0 ||
      mkdirs(directory) < 0 ||
      join(api_socket, sizeof api_socket, directory, "herdr.sock") < 0 ||
      join(client_socket, sizeof client_socket, directory, "herdr-client.sock") < 0)
    return -1;
  int written = snprintf(leaf, sizeof leaf, "fixture.%s.pid", session);
  return written >= 0 && (size_t)written < sizeof leaf &&
                 join(pid_path, sizeof pid_path, state, leaf) == 0 ? 0 : -1;
}

static int bind_unix(const char *path) {
  int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  struct sockaddr_un address;
  if (fd < 0) return -1;
  memset(&address, 0, sizeof address); address.sun_family = AF_UNIX;
  if (strlen(path) >= sizeof address.sun_path) { close(fd); errno = ENAMETOOLONG; return -1; }
  strcpy(address.sun_path, path);
  if (bind(fd, (struct sockaddr *)&address, sizeof address) < 0 || listen(fd, 4) < 0) {
    close(fd); return -1;
  }
  return fd;
}

static int read_pid(pid_t *pid) {
  FILE *input = fopen(pid_path, "r"); long value;
  if (!input) return -1;
  int scanned = fscanf(input, "%ld", &value);
  int closed = fclose(input);
  int ok = scanned == 1 && closed == 0 && value > 1;
  if (!ok) return -1;
  *pid = (pid_t)value; return 0;
}

static int record_environment(const char *state, const char *name) {
  char path[4096];
  if (join(path, sizeof path, state, name) < 0) return -1;
  FILE *output = fopen(path, "w"); if (!output) return -1;
  const char *names[] = {"XDG_CONFIG_HOME", "XDG_STATE_HOME", "HOME",
    "HERDR_CONFIG_PATH", "HERDR_SOCKET_PATH", "HERDR_CLIENT_SOCKET_PATH", "HERDR_SESSION"};
  for (size_t i = 0; i < sizeof names / sizeof names[0]; ++i) {
    const char *value = getenv(names[i]);
    fprintf(output, "%s=%s\n", names[i], value ? value : "<unset>");
    if (value && (strcmp(names[i], "XDG_CONFIG_HOME") == 0 ||
                  strcmp(names[i], "XDG_STATE_HOME") == 0)) {
      char target[4096]; ssize_t length = readlink(value, target, sizeof target - 1);
      if (length >= 0) { target[length] = '\0'; fprintf(output, "%s_TARGET=%s\n", names[i], target); }
    }
  }
  return fclose(output);
}

static void cleanup(void) {
  if (api_fd >= 0) close(api_fd);
  if (client_fd >= 0) close(client_fd);
  if (*api_socket) unlink(api_socket);
  if (*client_socket) unlink(client_socket);
  if (*pid_path) unlink(pid_path);
}

static int emit_status(const char *session, const char *state, int json) {
  pid_t pid; int running = read_pid(&pid) == 0 && kill(pid, 0) == 0 && !has(state, "status-not-running");
  const char *socket_value = has(state, "status-wrong-socket") ? "/wrong/socket" : api_socket;
  const char *session_value = has(state, "status-wrong-session") ? "wrong" : session;
  if (has(state, "status-malformed")) { puts("{"); return 0; }
  if (json) {
    printf("{\"status\":\"%s\",\"running\":%s,\"version\":%s,\"protocol\":%s,"
           "\"capabilities\":null,\"compatible\":%s,\"socket\":\"%s\","
           "\"session\":\"%s\",\"restart_needed\":false}\n",
           running ? "running" : "not_running", running ? "true" : "false",
           running ? "\"0.8.2\"" : "null", running ? "20" : "null",
           running ? (has(state, "status-incompatible") ? "false" : "true") : "null",
           socket_value, session_value);
  } else if (running) {
    printf("status: running\nversion: 0.8.2\nprotocol: 20\ncompatible: yes\nsocket: %s\n", socket_value);
  } else printf("status: not running\nsocket: %s\n", socket_value);
  return 0;
}

static int serve(const char *session, const char *config, const char *state) {
  if (paths(session, config, state) < 0 ||
      record_environment(state, "fixture.backend-environment") < 0 ||
      has(state, "fail-before-ready")) return 73;
  FILE *pid = fopen(pid_path, "wx"); if (!pid) return 74;
  fprintf(pid, "%ld\n", (long)getpid()); if (fclose(pid) != 0) return 74;
  api_fd = bind_unix(api_socket); if (api_fd < 0) { cleanup(); return 75; }
  if (!has(state, "omit-client-socket")) client_fd = bind_unix(client_socket);
  if (client_fd < 0) { cleanup(); return 76; }
  if (has(state, "exit-after-ready")) { cleanup(); return 77; }
  struct sigaction action; memset(&action, 0, sizeof action); action.sa_handler = on_signal;
  sigemptyset(&action.sa_mask); sigaction(SIGTERM, &action, NULL); sigaction(SIGINT, &action, NULL);
  for (;;) { pause(); if (!stopping) continue; if (has(state, "ignore-term")) { stopping = 0; continue; } break; }
  cleanup(); return 0;
}

int main(int argc, char **argv) {
  if (argc < 4 || strcmp(argv[1], "--session") != 0) return 64;
  const char *config = getenv("XDG_CONFIG_HOME"), *state = getenv("XDG_STATE_HOME");
  if (!config) return 65;
  if (!state) return 66;
  if (mkdirs(config) < 0) return 67;
  if (mkdirs(state) < 0) return 68;
  if (paths(argv[2], config, state) < 0) return 69;
  if (strcmp(argv[3], "status") == 0 && (argc == 5 || argc == 6) && strcmp(argv[4], "server") == 0)
    return emit_status(argv[2], state, argc == 6 && strcmp(argv[5], "--json") == 0);
  if (argc >= 5 && strcmp(argv[3], "workspace") == 0 && strcmp(argv[4], "create") == 0) {
    if (record_environment(state, "fixture.client-environment") < 0) return 78;
    puts("{\"id\":\"cli:workspace:create\",\"result\":{\"type\":\"workspace_created\",\"workspace\":{},\"tab\":{},\"root_pane\":{\"pane_id\":\"w:p\",\"terminal_id\":\"t\",\"workspace_id\":\"w\",\"tab_id\":\"tab\",\"focused\":true,\"agent_status\":\"unknown\",\"revision\":1}}}");
    return 0;
  }
  if (argc >= 5 && strcmp(argv[3], "agent") == 0 && strcmp(argv[4], "start") == 0) {
    if (record_environment(state, "fixture.client-environment") < 0) return 78;
    struct timespec delay = {.tv_sec = 0, .tv_nsec = 200000000};
    nanosleep(&delay, NULL);
    puts("{\"id\":\"cli:agent:start\",\"result\":{\"type\":\"agent_started\",\"agent\":{\"terminal_id\":\"t\",\"name\":\"probe\",\"agent\":\"codex\",\"agent_status\":\"idle\",\"workspace_id\":\"w\",\"tab_id\":\"tab\",\"pane_id\":\"w:p\",\"focused\":true,\"interactive_ready\":true,\"revision\":2},\"argv\":[\"codex\"]}}");
    return 0;
  }
  if (strcmp(argv[3], "server") != 0) return 64;
  if (argc == 5 && strcmp(argv[4], "stop") == 0) { pid_t pid; return read_pid(&pid) == 0 && kill(pid, SIGTERM) == 0 ? 0 : 1; }
  return argc == 4 ? serve(argv[2], config, state) : 64;
}
