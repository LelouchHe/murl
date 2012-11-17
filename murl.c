#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <unistd.h>
#include <curl/curl.h>

#include "murl.h"

struct connect_info_t
{
    CURL *easy;
    const char *url;
    long timeout_ms;
    const char *cookie;

    char *buf;
    size_t size;
    size_t len;

    int status;
    char error[CURL_ERROR_SIZE];
};

struct murl_t
{
    CURLM *multi;

    struct connect_info_t *conns;
    size_t size;
    size_t num;

    int epoll_fd;
    struct epoll_event *events;

    int run;
    long timeout_ms;
};

struct sock_info_t
{
    curl_socket_t sock_fd;
    CURL *easy;

    int event;

    struct murl_t *murl;
};

static void remove_sock(struct sock_info_t *sock)
{
    if (sock != NULL)
    {
        // epoll delete closed sock itself
//      epoll_ctl(sock->murl->epoll_fd, EPOLL_CTL_DEL, sock->sock_fd, NULL);
//      close(sock_fd);
    }
    free(sock);
}

static void set_sock(struct sock_info_t *sock, CURL *easy, curl_socket_t sock_fd, int what, struct murl_t *murl)
{
    struct epoll_event event;

    if (sock->event != 0)
        epoll_ctl(murl->epoll_fd, EPOLL_CTL_DEL, sock->sock_fd, NULL);

    sock->easy = easy;
    sock->sock_fd = sock_fd;
    sock->event = 0;

    if (what & CURL_POLL_IN)
        sock->event |= EPOLLIN;
    if (what & CURL_POLL_OUT)
        sock->event |= EPOLLOUT;

    event.events = sock->event;
    event.data.fd = sock_fd;

    epoll_ctl(murl->epoll_fd, EPOLL_CTL_ADD, sock->sock_fd, &event);
}

static void add_sock(CURL *easy, curl_socket_t sock_fd, int what, struct murl_t *murl)
{
    struct sock_info_t *sock = (struct sock_info_t *)malloc(sizeof (struct sock_info_t));
    sock->murl = murl;
    set_sock(sock, easy, sock_fd, what, murl);
    curl_multi_assign(murl->multi, sock_fd, sock);
}

static int sock_cb(CURL *easy, curl_socket_t sock_fd, int what, void *cbp, void *sockp)
{
    struct murl_t *murl = (struct murl_t *)cbp;
    struct sock_info_t *sock = (struct sock_info_t *)sockp;

    if (what == CURL_POLL_REMOVE)
    {
        remove_sock(sock);
        sock = NULL;
    }
    else if (sock == NULL)
        add_sock(easy, sock_fd, what, murl);
    else
        set_sock(sock, easy, sock_fd, what, murl);

    return 0;
}

static int timer_cb(CURLM *multi, long timeout_ms, void *timer_data)
{
    struct murl_t *murl = (struct murl_t *)timer_data;

    murl->timeout_ms = timeout_ms;

    return 0;
}

struct murl_t *murl_malloc(int size)
{
    int i;
    struct murl_t *murl = (struct murl_t *)malloc(sizeof (struct murl_t));

    murl->conns = (struct connect_info_t *)malloc(size * sizeof (struct connect_info_t));
//    memset(murl->conns, 0, size * sizeof (struct connect_info_t));
    for (i = 0; i < size; i++)
    {
        murl->conns[i].status = MURLS_NONEXIST;
        murl->conns[i].easy = NULL;
    }
    murl->size = size;
    murl->num = 0;

    murl->epoll_fd = epoll_create(size);
    murl->events = (struct epoll_event *)malloc(size * sizeof (struct epoll_event));
    murl->run = 0;

    murl->multi = curl_multi_init();
    curl_multi_setopt(murl->multi, CURLMOPT_SOCKETFUNCTION, sock_cb);
    curl_multi_setopt(murl->multi, CURLMOPT_SOCKETDATA, murl);
    curl_multi_setopt(murl->multi, CURLMOPT_TIMERFUNCTION, timer_cb);
    curl_multi_setopt(murl->multi, CURLMOPT_TIMERDATA, murl);

    return murl;
}

static int clean(struct murl_t *murl, int should_cleanup)
{
    size_t i;
    struct connect_info_t *conns = murl->conns;
    for (i = 0; i < murl->size && murl->num > 0; i++)
    {
        if (conns[i].status != MURLS_NONEXIST)
        {
            curl_multi_remove_handle(murl->multi, conns[i].easy);
            if (should_cleanup == 0)
                curl_easy_reset(conns[i].easy);
            else
                curl_easy_cleanup(conns[i].easy);

            free((char *)conns[i].url);
            conns[i].url = NULL;
            conns[i].status = MURLS_NONEXIST;
            conns[i].error[0] = '\0';
            conns[i].len = 0;

            murl->num--;
        }
    }

    return 0;
}

int murl_free(struct murl_t *murl)
{
    if (murl != NULL)
    {
        clean(murl, 1);

        free(murl->events);
        murl->events = NULL;
        close(murl->epoll_fd);

        free(murl->conns);
        murl->conns = NULL;
        murl->size = 0;

        free(murl);
    }

    return 0;
}

int murl_clean(struct murl_t *murl)
{
    if (murl != NULL)
        return clean(murl, 0);
    else
        return MURLE_NULL;
}

static size_t write_cb(void *ptr, size_t size, size_t nmemb, void *data)
{
    struct connect_info_t *conn = (struct connect_info_t *)data;

    if (conn->len + size * nmemb > conn->size)
        return 0;

    memcpy(conn->buf + conn->len, ptr, size * nmemb);
    conn->len += size * nmemb;

    return size * nmemb;
}

int murl_add_url(struct murl_t *murl, const char *url, char *buf, int size, long timeout_ms, const char *cookie)
{
    if (murl == NULL)
        return MURLE_NULL;

    struct connect_info_t *conns = murl->conns;
    size_t i;
    for (i = 0; i < murl->size; i++)
    {
        if (conns[i].status == MURLS_NONEXIST)
            break;
    }

    if (i == murl->size)
        return MURLE_TOOMUCH_URL;

    if (conns[i].easy == NULL)
        conns[i].easy = curl_easy_init();
    conns[i].url = strdup(url);
    conns[i].timeout_ms = timeout_ms;
    conns[i].cookie = NULL;
    if (cookie != NULL)
        conns[i].cookie = strdup(cookie);
//    memset(buf, 0, size);
    conns[i].buf = buf + sizeof (uint32_t);
    conns[i].size = size - sizeof (uint32_t);
    conns[i].len = 0;
    conns[i].status = MURLS_TOPERFORM;
    conns[i].error[0] = '\0';

    curl_easy_setopt(conns[i].easy, CURLOPT_URL, conns[i].url);
    curl_easy_setopt(conns[i].easy, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(conns[i].easy, CURLOPT_WRITEDATA, &conns[i]);
    curl_easy_setopt(conns[i].easy, CURLOPT_ERRORBUFFER, conns[i].error);
    curl_easy_setopt(conns[i].easy, CURLOPT_PRIVATE, &conns[i]);
    curl_easy_setopt(conns[i].easy, CURLOPT_TIMEOUT_MS, conns[i].timeout_ms);
    if (cookie != NULL)
        curl_easy_setopt(conns[i].easy, CURLOPT_COOKIE, conns[i].cookie);


    curl_multi_add_handle(murl->multi, conns[i].easy);
    murl->num++;

    return MURLE_OK;
}

static struct connect_info_t *find_url(struct connect_info_t *conns, size_t size, size_t num, const char *url)
{
    size_t i;
    size_t check_num;

    for (i = 0, check_num = 0; i < size && check_num < num; i++)
    {
        if (conns[i].status != MURLS_NONEXIST)
        {
            check_num++;
            if (strcmp(conns[i].url, url) == 0)
                return &conns[i];
        }
    }

    return NULL;
}

int murl_remove_url(struct murl_t *murl, const char *url)
{
    if (murl == NULL)
        return MURLE_NULL;

    struct connect_info_t *conn = find_url(murl->conns, murl->size, murl->num, url);
    if (conn == NULL)
        return MURLE_INVALID_URL;

    curl_multi_remove_handle(murl->multi, conn->easy);
    curl_easy_reset(conn->easy);

    free((char *)conn->url);
    conn->url = NULL;

    murl->num--;

    return MURLE_OK;
}

int murl_url_status(struct murl_t *murl, const char *url, char *error, int size)
{
    if (murl == NULL)
        return -1;

    struct connect_info_t *conn = find_url(murl->conns, murl->size, murl->num, url);
    if (conn == NULL)
        return MURLE_INVALID_URL;

    if (error != NULL)
        strncpy(error, conn->error, size);

    return conn->status;
}

static long get_ms(struct timeval ts, struct timeval te)
{
    return (te.tv_sec - ts.tv_sec) * 1000 + (te.tv_usec - ts.tv_usec) / 1000;
}

static long min(long a, long b)
{
    return a < b ? a : b;
}

static int add_timeout(struct connect_info_t *conns, size_t size, size_t num, long timeout_ms)
{
    size_t i, check_num;

    for (i = 0, check_num = 0; i < size && check_num < num; i++)
    {
        if (conns[i].status != MURLS_NONEXIST)
        {
            check_num++;
            if (conns[i].timeout_ms > timeout_ms)
                curl_easy_setopt(conns[i].easy, CURLOPT_TIMEOUT_MS, timeout_ms);
        }
    }

    return 0;
}

static int check(struct murl_t *murl, int once)
{
    CURLMsg *msg;
    int msgs_left;
    struct connect_info_t *conn;
    struct connect_info_t *conns;

    int timeout = 0, overflow = 0;
    size_t i, check_num;

    while ((msg = curl_multi_info_read(murl->multi, &msgs_left)) != NULL)
    {
        if (msg->msg == CURLMSG_DONE)
        {
            curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, &conn);
            if (msg->data.result == CURLE_OPERATION_TIMEDOUT)
            {
                conn->status = MURLE_TIMEOUT;
                timeout = 1;
            }
            else if (msg->data.result == CURLE_WRITE_ERROR)
            {
                conn->status = MURLE_OVERFLOW;
                overflow = 1;
            }
            else
            {
                conn->status = MURLS_OK;
                conn->error[0] = '\0';
            }
            *(uint32_t *)(conn->buf - sizeof (uint32_t)) = conn->len;
        }
    }

    if (once == 1)
    {
        if (timeout == 1 && overflow == 1)
            return MURLE_MULTI;
        else if (timeout == 1)
            return MURLE_TIMEOUT;
        else if (overflow == 1)
            return MURLE_OVERFLOW;
        else
            return MURLE_OK;
    }
    else
    {
        conns = murl->conns;
        for (i = 0, check_num = 0; i < murl->size && check_num < murl->num; i++)
        {
            if (conns[i].status != MURLS_NONEXIST)
            {
                check_num++;
                if (conns[i].status == MURLS_TOPERFORM)
                {
                    conns[i].status = MURLE_TIMEOUT;
                    *(uint32_t *)(conns[i].buf - sizeof (uint32_t)) = conns[i].len;
                }
            }
        }

        return murl->run;
    }
}


static int perform(struct murl_t *murl, long timeout_ms, int once)
{
    struct timeval ts, te;

    long wait_time;
    struct epoll_event *events = murl->events;

    gettimeofday(&ts, NULL);

    if (once == 1)
        add_timeout(murl->conns, murl->size, murl->num, timeout_ms);

    curl_multi_socket_action(murl->multi, CURL_SOCKET_TIMEOUT, 0, &murl->run);
    while (murl->run > 0)
    {
        gettimeofday(&te, NULL);
        wait_time = min(murl->timeout_ms, timeout_ms - get_ms(ts, te));

        if (wait_time < 0)
            break;
    
        int num = epoll_wait(murl->epoll_fd, events, murl->size, wait_time);
        if (num < 0)
            break;
        else if (num == 0)
            curl_multi_socket_action(murl->multi, CURL_SOCKET_TIMEOUT, 0, &murl->run);

        int i;
        for (i = 0; i < num; i++)
        {
            int action = 0;
            if (events[i].events & EPOLLIN)
                action |= CURL_CSELECT_IN;
            if (events[i].events & EPOLLOUT)
                action |= CURL_CSELECT_OUT;

            curl_multi_socket_action(murl->multi, events[i].data.fd, action, &murl->run);
        }
    }

    return check(murl, once);
}

int murl_get_contents(struct murl_t *murl, long timeout_ms)
{
    if (murl == NULL)
        return MURLE_NULL;

    return perform(murl, timeout_ms, 1);
}

int murl_perform(struct murl_t *murl, long timeout_ms)
{
    if (murl == NULL)
        return MURLE_NULL;

    return perform(murl, timeout_ms, 0);
}

int murl_get_url(const char *url, char *buf, int size, long timeout_ms, const char *cookie)
{
    struct connect_info_t conn;
    int ret;

    conn.easy = curl_easy_init();
    conn.url = url;
    conn.buf = buf + sizeof (uint32_t);
    conn.size = size - sizeof (uint32_t);
    conn.len = 0;
    conn.status = MURLS_TOPERFORM;
    conn.error[0] = '\0';

    curl_easy_setopt(conn.easy, CURLOPT_URL, conn.url);
    curl_easy_setopt(conn.easy, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(conn.easy, CURLOPT_WRITEDATA, &conn);
    curl_easy_setopt(conn.easy, CURLOPT_ERRORBUFFER, conn.error);
    curl_easy_setopt(conn.easy, CURLOPT_TIMEOUT_MS, timeout_ms);
    if (cookie != NULL)
        curl_easy_setopt(conn.easy, CURLOPT_COOKIE, cookie);

    ret = curl_easy_perform(conn.easy);

    curl_easy_cleanup(conn.easy);

    *(uint32_t *)buf = conn.len;

    if (ret == CURLE_OPERATION_TIMEDOUT)
        return MURLE_TIMEOUT;
    else if (ret == CURLE_WRITE_ERROR)
        return MURLE_OVERFLOW;
    else
        return MURLE_OK;
}

