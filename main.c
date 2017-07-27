#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>

#include <getopt.h>

#include <rte_byteorder.h>
#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_pci.h>

#include <rte_version.h>

#define NB_MBUF 8192
#define MBUF_SIZE (2048 + sizeof (struct rte_mbuf) + RTE_PKTMBUF_HEADROOM)
#define MBUF_CACHE_SIZE 32
#define NB_RX_DESC 128
#define NB_TX_DESC 512
#define RX_PTHRESH 8
#define RX_HTHRESH 8
#define RX_WTHRESH 4
#define TX_PTHRESH 36
#define TX_HTHRESH 0
#define TX_WTHRESH 0
#define MAX_BURST_SIZE 32

#define RTE_FAIL(...) rte_exit(EXIT_FAILURE, __VA_ARGS__)

struct element {
    enum { EMPTY_ELEM, RECV_ELEM, GEN_ELEM, ECHO_ELEM } role;
    unsigned port_no;
    unsigned rx_queue_no;
    unsigned tx_queue_no;
    unsigned lcore_socket;
    unsigned n_recvd;
    unsigned n_sent;
    unsigned n_dropped;
    struct rte_mempool *pktmbuf_pool;
} __rte_cache_aligned;

static void usage(const char *progName)
{
    fprintf(stderr, "Usage: %s [EAL arguments] ELEM [ELEM [...]]\n", progName);
    fputs("where\tELEM is a CORE:ROLE:PORT triple\n"
          "\tCORE is the number of a logical core\n"
          "\tROLE is\n"
          "\t\tr  for receiver\n"
          "\t\tg  for generator\n"
          "\t\te  for echoer\n"
          "\tPORT is the DPDK-enabled ethernet port number\n",
          stderr);
}

static bool parse_elements(int argc, char *argv[],
                           struct element elements[RTE_MAX_LCORE],
                           unsigned n_ports)
{
    if (argc <= 1)
        return false;
    for (int i = 1; i < argc; ++i) {
        unsigned lcore_id;
        char role;
        unsigned port_no;
        int n = 0;
        sscanf(argv[i], "%u:%c:%u%n", &lcore_id, &role, &port_no, &n);
        if (argv[i][n] != '\0') {
            fprintf(stderr,
                    "Error: %s is not a valid element specification.\n",
                    argv[i]);
            return false;
        }
        if (!rte_lcore_is_enabled(lcore_id)) {
            fprintf(stderr,
                    "Error: core %d is not present in cores mask (in %s).\n",
                    lcore_id, argv[i]);
            return false;
        }
        if (elements[lcore_id].role != EMPTY_ELEM) {
            fprintf(stderr, "Error: core %d assigned to multiple elements!\n",
                    lcore_id);
            return false;
        }
        struct element *elem = &elements[lcore_id];
        switch (role) {
            case 'r':
                elem->role = RECV_ELEM;
                break;
            case 'g':
                elem->role = GEN_ELEM;
                break;
            case 'e':
                elem->role = ECHO_ELEM;
                break;
            default:
                fprintf(stderr, "Error: unknown role '%c' (in %s).\n", role,
                        argv[i]);
                return false; 
        }
        if (port_no >= n_ports) {
            fprintf(stderr, "Error: no such port: %u (in %s)\n", port_no,
                    argv[i]);
            return false;
        }
        elem->port_no = port_no;
    }
    return true;
}

extern void links_are_up(size_t n, bool is_active_port[n]);
void links_are_up(size_t n, bool is_active_port[n])
{
    for (unsigned i = 0; i < n; ++i) {
        if (is_active_port[i]) {
            struct rte_eth_link link = { 0 };
            rte_eth_link_get(i, &link);
            if (!link.link_status) {
                fprintf(stderr, "Link is down on port %u\n", i);
            }
        }
    }
}

static volatile bool should_continue = true;

static void handle_signal(int signal)
{
    switch (signal) {
        case SIGINT:
            puts("\nCaught SIGINT");
            break;
        case SIGTERM:
            puts("Caught SIGTERM");
            break;
        case SIGHUP:
            puts("Caught SIGHUP");
        default:
            // Unknown signal, don't exit
            fprintf(stderr, "Caught unexpected signal %d, ignoring.\n",
                    signal);
            return;
    }
    if (should_continue) {
        should_continue = false;
        puts("Stopping all threads, please wait or do Ctrl-C for an emergency"
             " stop.");
    } else if (signal == SIGINT) {
        // We were already exiting, so do an emergency exit
        RTE_FAIL("Aborting, as asked.\n");
    }
}

static void send_burst(struct element *self, unsigned n,
                       struct rte_mbuf *pkts[n])
{
    unsigned n_sent =
        rte_eth_tx_burst(self->port_no, self->tx_queue_no, pkts, n);
    self->n_sent += n_sent;
    self->n_dropped += (n - n_sent);
    while (n_sent < n)
        rte_pktmbuf_free(pkts[n_sent++]);
}

static int run_recv(struct element *self)
{
    struct rte_mbuf *pkts[MAX_BURST_SIZE];
    RTE_LOG(INFO, USER1, "Started receiver on lcore %u, port %u, RXQ %u.\n",
            rte_lcore_id(), self->port_no, self->rx_queue_no);
    while (should_continue) {
        unsigned n =
            rte_eth_rx_burst(
                self->port_no, self->rx_queue_no, pkts, MAX_BURST_SIZE);
        self->n_recvd += n;
        for (unsigned i = 0; i < n; ++i)
            rte_pktmbuf_free(pkts[i]);
    }
    return 0;
}

static int run_gen(struct element *self)
{
    struct rte_mbuf *pkts[MAX_BURST_SIZE];
    struct ether_addr src;
    rte_eth_macaddr_get(self->port_no, &src);
    RTE_LOG(INFO, USER1, "Started generator on lcore %u, port %u, TXQ %u.\n",
            rte_lcore_id(), self->port_no, self->tx_queue_no);
    while (should_continue) {
        for (unsigned i = 0; i < MAX_BURST_SIZE; ++i) {
            pkts[i] = rte_pktmbuf_alloc(self->pktmbuf_pool);
            if (!pkts[i])
                RTE_FAIL("Error allocating packet.\n");
            struct ether_hdr *eth = rte_pktmbuf_mtod(pkts[i],
                                                     struct ether_hdr *);
            ether_addr_copy(&src, &eth->s_addr);
            for (unsigned j = 0; j < ETHER_ADDR_LEN; ++j)
                eth->d_addr.addr_bytes[j] = 0xff;
            eth->ether_type = rte_bswap16(ETHER_TYPE_IPv4);
            struct ipv4_hdr *ip = (struct ipv4_hdr *) &eth[1];
            ip->src_addr = rte_bswap32(0x0a000000 + i);
#if RTE_VERSION < RTE_VERSION_NUM(1,8,0,0)
            pkts[i]->pkt.pkt_len = 64;
            pkts[i]->pkt.data_len = 64;
            pkts[i]->pkt.next = NULL;
            pkts[i]->pkt.nb_segs = 1;
#else
            pkts[i]->pkt_len = 64;
            pkts[i]->data_len = 64;
            pkts[i]->next = NULL;
            pkts[i]->nb_segs = 1;
#endif
        }
        send_burst(self, MAX_BURST_SIZE, pkts);
    }
    return 0;
}

static int run_echo(struct element *self)
{
    struct rte_mbuf *pkts[MAX_BURST_SIZE];
    struct ether_addr src;
    rte_eth_macaddr_get(self->port_no, &src);
    RTE_LOG(INFO, USER1,
            "Started echoer on lcore %u, port %u, RXQ %u, TXQ %u.\n",
            rte_lcore_id(), self->port_no, self->rx_queue_no,
            self->tx_queue_no);
    while (should_continue) {
        unsigned n =
            rte_eth_rx_burst(
                self->port_no, self->rx_queue_no, pkts, MAX_BURST_SIZE);
        self->n_recvd += n;
        for (unsigned i = 0; i < n; ++i) {
            struct rte_mbuf *m = pkts[i];
            rte_prefetch0(rte_pktmbuf_mtod(m, void *));
            
            struct ether_hdr *eth = rte_pktmbuf_mtod(m, struct ether_hdr *);
            ether_addr_copy(&eth->s_addr, &eth->d_addr);
            ether_addr_copy(&src, &eth->s_addr);
        }
        send_burst(self, n, pkts);
    }
    return 0;
}

static int run_element(struct element *self)
{
    switch (self->role) {
        case RECV_ELEM:
            return run_recv(self);
        case GEN_ELEM:
            return run_gen(self);
        case ECHO_ELEM:
            return run_echo(self);
        case EMPTY_ELEM:
            return 0;
    }
    return 0; // Never reached, gcc bug
}

int main(int argc, char *argv[])
{
    // Initialize EAL
    int n_eal_args = rte_eal_init(argc, argv);
    if (n_eal_args < 0)
        RTE_FAIL("Invalid EAL arguments.\n");
    argc -= n_eal_args;
    argv += n_eal_args;

    // Probe the PCI bus
    if (rte_eal_pci_probe())
        RTE_FAIL("Cannot probe the PCI bus.\n");

    // Get the number of DPDK-enabled ethernet ports
    const unsigned n_ports = rte_eth_dev_count();
    if (n_ports == 0)
        RTE_FAIL("No DPDK-enabled ethernet port found.\n");

    // Parse elements
    struct element elements[RTE_MAX_LCORE];
    memset(elements, 0, sizeof elements);
    if (!parse_elements(argc, argv, elements, n_ports)) {
        usage(argv[-n_eal_args]);
        RTE_FAIL("Invalid element specification.\n");
    }
    unsigned lcore_id;
    unsigned n_rx_queues[n_ports];
    memset(n_rx_queues, 0, n_ports * sizeof (int));
    unsigned n_tx_queues[n_ports];
    memset(n_tx_queues, 0, n_ports * sizeof (int));
    RTE_LCORE_FOREACH(lcore_id) {
        struct element *elem = &elements[lcore_id];
        switch (elem->role) {
            case RECV_ELEM:
                printf("lcore %u is acting as a receiver", lcore_id);
                elem->rx_queue_no = n_rx_queues[elem->port_no]++;
                break;
            case GEN_ELEM:
                printf("lcore %u is acting as a generator", lcore_id);
                elem->tx_queue_no = n_tx_queues[elem->port_no]++;
                break;
            case ECHO_ELEM:
                printf("lcore %u is acting as an echoer", lcore_id);
                elem->rx_queue_no = n_rx_queues[elem->port_no]++;
                elem->tx_queue_no = n_tx_queues[elem->port_no]++;
                break;
            case EMPTY_ELEM:
                RTE_FAIL("lcore %u was not given any role!\n", lcore_id);
        }
        printf(" on port %u (rx %u, tx %u).\n", elem->port_no,
               elem->rx_queue_no, elem->tx_queue_no);
    }

    // Mark active ports
    bool is_active_port[n_ports];
    for (unsigned i = 0; i < n_ports; ++i)
        is_active_port[i] = n_rx_queues[i] || n_tx_queues[i];

    // Initialize active ports
    const struct rte_eth_conf dev_conf = {
        .rxmode = {
            .mq_mode = ETH_MQ_RX_RSS,
        },
        .rx_adv_conf = {
            .rss_conf = {
                .rss_key = NULL,
                .rss_hf = ETH_RSS_IP,
            },
        },
    };
    for (unsigned i = 0; i < n_ports; ++i)
        if (is_active_port[i]) {
            if (rte_eth_dev_configure(i, n_rx_queues[i], n_tx_queues[i],
                                      &dev_conf) < 0)
                RTE_FAIL(
                    "Cannot initialize port %u with %u RX and %u TX queues.\n",
                    i, n_rx_queues[i], n_tx_queues[i]);
        }

    // Warn about wrong sockets
    RTE_LCORE_FOREACH(lcore_id) {
        struct element *elem = &elements[lcore_id];
        unsigned lcore_socket_id = rte_lcore_to_socket_id(lcore_id);
        unsigned port_socket_id =
            rte_eth_dev_socket_id(elem->port_no);
        if (lcore_socket_id != port_socket_id)
            fprintf(stderr,
                    "Warning: lcore %u is on socket %u, but acting on port %u"
                    " on socket %u.\n",
                    lcore_id, lcore_socket_id, elem->port_no,
                    port_socket_id);
    }

    // Mark active sockets
    unsigned max_socket_id = 0;
    RTE_LCORE_FOREACH(lcore_id) {
        unsigned socket_id = rte_lcore_to_socket_id(lcore_id);
        if (socket_id > max_socket_id)
            max_socket_id = socket_id;
    }
    const unsigned n_sockets = max_socket_id + 1;
    bool is_active_socket[n_sockets];
    memset(is_active_socket, 0, n_sockets * sizeof (bool));
    RTE_LCORE_FOREACH(lcore_id) {
        is_active_socket[rte_lcore_to_socket_id(lcore_id)] = true;
    }

    // Create the mbuf pools
    struct rte_mempool *pktmbuf_pools[n_sockets];
    memset(pktmbuf_pools, 0, n_sockets * sizeof (struct rte_mempool *));
    for (unsigned i = 0; i < n_sockets; ++i) {
        if (is_active_socket[i]) {
            char name[64];
            snprintf(name, 64, "mbuf_pool_%u", i);
            pktmbuf_pools[i] =
                rte_mempool_create(
                    name, NB_MBUF, MBUF_SIZE, MBUF_CACHE_SIZE,
                    sizeof (struct rte_pktmbuf_pool_private),
                    rte_pktmbuf_pool_init, NULL,
                    rte_pktmbuf_init, NULL,
                    (int) i, 0);
            if (!pktmbuf_pools[i])
                RTE_FAIL("Cannot init mbuf pool on socket %u.\n", i);
        }
    }

    // Create send/recv queues
    const struct rte_eth_rxconf rx_conf = {
        .rx_thresh = {
            .pthresh = RX_PTHRESH,
            .hthresh = RX_HTHRESH,
            .wthresh = RX_WTHRESH,
        },
    };
    const struct rte_eth_txconf tx_conf = {
        .tx_thresh = {
            .pthresh = TX_PTHRESH,
            .hthresh = TX_HTHRESH,
            .wthresh = TX_WTHRESH,
        },
        .tx_free_thresh = 0, /* use default value */
        .tx_rs_thresh = 0, /* use default value */
        .txq_flags = ETH_TXQ_FLAGS_NOMULTSEGS | ETH_TXQ_FLAGS_NOOFFLOADS,
    };
    RTE_LCORE_FOREACH(lcore_id) {
        struct element *elem = &elements[lcore_id];
        elem->lcore_socket = rte_lcore_to_socket_id(lcore_id);
        elem->pktmbuf_pool = pktmbuf_pools[elem->lcore_socket];
        switch (elem->role) {
            case RECV_ELEM:
                if (rte_eth_rx_queue_setup(
                        elem->port_no, elem->rx_queue_no,
                        NB_RX_DESC, elem->lcore_socket,
                        &rx_conf, elem->pktmbuf_pool) != 0)
                    RTE_FAIL("Cannot initialize RX queue %u of "
                             "port %u for lcore %u.\n",
                             elem->rx_queue_no, elem->port_no,
                             lcore_id);
                break;
            case GEN_ELEM:
                if (rte_eth_tx_queue_setup(
                        elem->port_no, elem->tx_queue_no,
                        NB_TX_DESC, elem->lcore_socket,
                        &tx_conf)  != 0)
                    RTE_FAIL("Cannot initialize TX queue %u of "
                             "port %u for lcore %u.\n",
                             elem->tx_queue_no, elem->port_no,
                             lcore_id);
                break;
            case ECHO_ELEM:
                if (rte_eth_rx_queue_setup(
                        elem->port_no, elem->rx_queue_no,
                        NB_RX_DESC, elem->lcore_socket,
                        &rx_conf, elem->pktmbuf_pool) != 0)
                    RTE_FAIL("Cannot initialize RX queue %u of "
                             "port %u for lcore %u.\n",
                             elem->rx_queue_no, elem->port_no,
                             lcore_id);
                if (rte_eth_tx_queue_setup(
                        elem->port_no, elem->tx_queue_no,
                        NB_TX_DESC, elem->lcore_socket,
                        &tx_conf) != 0)
                    RTE_FAIL("Cannot initialize TX queue %u of "
                             "port %u for lcore %u.\n",
                             elem->tx_queue_no, elem->port_no,
                             lcore_id);
                break;
            case EMPTY_ELEM:
                break;
        }
    }

    // Start active ports
    for (unsigned i = 0; i < n_ports; ++i)
        if (is_active_port[i]) {
            int status = rte_eth_dev_start(i);
            if (status < 0)
                RTE_FAIL("Error %d starting port %u.\n", status, i);
            rte_eth_promiscuous_enable(i);
        }

    // Check active ports are up
    //links_are_up(n_ports, is_active_port);

    // Register signals handler
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = handle_signal;
    sigfillset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL))
        RTE_FAIL("Cannot register SIGINT handler.\n");
    if (sigaction(SIGTERM, &sa, NULL))
        RTE_FAIL("Cannot register SIGTERM handler.\n");
    if (sigaction(SIGHUP, &sa, NULL))
        RTE_FAIL("Cannot register SIGHUP handler.\n");

    // Launch elements
    uint64_t init_cycles = rte_get_timer_cycles();
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        rte_eal_remote_launch((int (*)(void *)) run_element,
                              &elements[lcore_id], lcore_id);
    }
    run_element(&elements[rte_lcore_id()]);

    // Wait for termination of other threads
    int status = EXIT_SUCCESS;
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        int exit_code = rte_eal_wait_lcore(lcore_id);
        if (exit_code != 0) {
            RTE_LOG(ERR, USER1, "lcore %u exited with error %d\n", lcore_id,
                    exit_code);
            status = EXIT_FAILURE;
        }
    }

    // Print stats
    uint64_t end_cycles = rte_get_timer_cycles();
    unsigned rx_pkts[n_ports];
    memset(rx_pkts, 0, n_ports * sizeof (unsigned));
    unsigned tx_pkts[n_ports];
    memset(tx_pkts, 0, n_ports * sizeof (unsigned));
    unsigned drop_pkts[n_ports];
    memset(drop_pkts, 0, n_ports * sizeof (unsigned));
    RTE_LCORE_FOREACH(lcore_id) {
        struct element *elem = &elements[lcore_id];
        rx_pkts[elem->port_no] += elem->n_recvd;
        tx_pkts[elem->port_no] += elem->n_sent;
        drop_pkts[elem->port_no] += elem->n_dropped;
        printf("%u: RX %12u, TX %12u, D %12u\n", lcore_id,
               elem->n_recvd, elem->n_sent, elem->n_dropped);
    }
    puts("Port      RX / pkts   RX / Mpps      TX / pkts    Drop / pkts"
         "   TX / Mpps");
    for (unsigned i = 0; i < n_ports; ++i) {
        double n_seconds =
            (double) (end_cycles - init_cycles) / (double) rte_get_timer_hz();
        double rx_speed = (double) rx_pkts[i] / n_seconds / 10e6;
        double tx_speed = (double) tx_pkts[i] / n_seconds / 10e6;
        printf("%2u     %12u   %9.3f   %12u   %12u   %9.3f\n", i, rx_pkts[i],
               rx_speed, tx_pkts[i], drop_pkts[i], tx_speed);
    }

    return status;
}
