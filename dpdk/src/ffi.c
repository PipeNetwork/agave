#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>

struct agave_dpdk_port {
  uint16_t port_id;
  uint16_t tx_queues;
  struct rte_mempool *mempool;
  struct rte_ether_addr mac;
};

static unsigned int agave_round_down_pow2(unsigned int x) {
  if (x == 0) {
    return 0;
  }
  unsigned int p = 1;
  while ((p << 1) != 0 && (p << 1) <= x) {
    p <<= 1;
  }
  return p;
}

static unsigned int agave_calc_mempool_cache_size(uint32_t mbuf_count, uint16_t rx_queues) {
  // `rte_pktmbuf_pool_create()` uses per-lcore caches. Using an oversized cache for a small pool
  // can cause pool creation to fail and/or starve the global pool. Keep the cache conservative and
  // disable it entirely for very small pools.
  unsigned int desired = 256;
  unsigned int lcores = (unsigned int)rx_queues + 1u; // main thread + RX queues
  if (lcores == 0) {
    lcores = 1;
  }

  // Keep aggregate cache <= ~50% of the pool.
  uint32_t max_cache = mbuf_count / (lcores * 2u);
  if (max_cache == 0) {
    return 0;
  }
  if (desired > max_cache) {
    desired = (unsigned int)max_cache;
  }

  if (desired > RTE_MEMPOOL_CACHE_MAX_SIZE) {
    desired = RTE_MEMPOOL_CACHE_MAX_SIZE;
  }

  desired = agave_round_down_pow2(desired);
  if (desired < 32) {
    return 0;
  }
  if ((uint32_t)desired >= mbuf_count) {
    return 0;
  }
  return desired;
}

int agave_dpdk_port_open(uint16_t port_id, uint16_t rx_queues, uint16_t tx_queues,
                         uint16_t rx_desc, uint16_t tx_desc, uint32_t mbuf_count,
                         uint16_t mbuf_data_size,
                         struct agave_dpdk_port **out_port) {
  if (out_port == NULL) {
    return -EINVAL;
  }

  struct agave_dpdk_port *port = (struct agave_dpdk_port *)calloc(1, sizeof(*port));
  if (port == NULL) {
    return -ENOMEM;
  }
  port->port_id = port_id;

  int socket_id = rte_eth_dev_socket_id(port_id);
  if (socket_id < 0) {
    socket_id = 0;
  }

  char pool_name[64];
  snprintf(pool_name, sizeof(pool_name), "agave_dpdk_mempool_%u", port_id);

  unsigned int cache_size = agave_calc_mempool_cache_size(mbuf_count, rx_queues);
  port->mempool = rte_pktmbuf_pool_create(pool_name, mbuf_count,
                                          /*cache_size=*/cache_size, /*priv_size=*/0,
                                          mbuf_data_size, socket_id);
  if (port->mempool == NULL) {
    free(port);
    return -ENOMEM;
  }

  struct rte_eth_conf port_conf;
  memset(&port_conf, 0, sizeof(port_conf));

  struct rte_eth_dev_info dev_info;
  memset(&dev_info, 0, sizeof(dev_info));
  rte_eth_dev_info_get(port_id, &dev_info);

  if (tx_queues == 0) {
    tx_queues = 1;
  }
  if (dev_info.max_tx_queues != 0 && tx_queues > dev_info.max_tx_queues) {
    tx_queues = dev_info.max_tx_queues;
    if (tx_queues == 0) {
      tx_queues = 1;
    }
  }

  if (rx_queues > 1) {
    if (dev_info.flow_type_rss_offloads == 0) {
      rte_mempool_free(port->mempool);
      free(port);
      return -ENOTSUP;
    }
    port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
    port_conf.rx_adv_conf.rss_conf.rss_key = NULL;
    port_conf.rx_adv_conf.rss_conf.rss_key_len = 0;
    port_conf.rx_adv_conf.rss_conf.algorithm = RTE_ETH_HASH_FUNCTION_DEFAULT;
    port_conf.rx_adv_conf.rss_conf.rss_hf = dev_info.flow_type_rss_offloads;
  } else {
    port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_NONE;
  }

  int ret = rte_eth_dev_configure(port_id, rx_queues, tx_queues, &port_conf);
  if (ret < 0) {
    rte_mempool_free(port->mempool);
    free(port);
    return ret;
  }

  ret = rte_eth_dev_adjust_nb_rx_tx_desc(port_id, &rx_desc, &tx_desc);
  if (ret < 0) {
    rte_mempool_free(port->mempool);
    free(port);
    return ret;
  }

  struct rte_eth_rxconf rx_conf = dev_info.default_rxconf;
  rx_conf.offloads = port_conf.rxmode.offloads;

  for (uint16_t q = 0; q < rx_queues; q++) {
    ret = rte_eth_rx_queue_setup(port_id, q, rx_desc, socket_id, &rx_conf, port->mempool);
    if (ret < 0) {
      rte_mempool_free(port->mempool);
      free(port);
      return ret;
    }
  }

  struct rte_eth_txconf tx_conf = dev_info.default_txconf;
  tx_conf.offloads = port_conf.txmode.offloads;

  for (uint16_t q = 0; q < tx_queues; q++) {
    ret = rte_eth_tx_queue_setup(port_id, q, tx_desc, socket_id, &tx_conf);
    if (ret < 0) {
      rte_mempool_free(port->mempool);
      free(port);
      return ret;
    }
  }

  ret = rte_eth_dev_start(port_id);
  if (ret < 0) {
    rte_mempool_free(port->mempool);
    free(port);
    return ret;
  }

  // Do not enable promiscuous mode by default. The validator traffic we care about is destined to
  // our MAC (unicast) or broadcast (ARP), and keeping promisc off improves compatibility and
  // reduces unintended traffic.
  rte_eth_macaddr_get(port_id, &port->mac);
  port->tx_queues = tx_queues;

  *out_port = port;
  return 0;
}

void agave_dpdk_port_close(struct agave_dpdk_port *port) {
  if (port == NULL) {
    return;
  }
  rte_eth_dev_stop(port->port_id);
  rte_eth_dev_close(port->port_id);
  if (port->mempool != NULL) {
    rte_mempool_free(port->mempool);
  }
  free(port);
}

void agave_dpdk_port_get_mac(const struct agave_dpdk_port *port, uint8_t out_mac[6]) {
  if (port == NULL || out_mac == NULL) {
    return;
  }
  memcpy(out_mac, &port->mac.addr_bytes[0], 6);
}

int agave_dpdk_port_get_link(const struct agave_dpdk_port *port, uint8_t *out_up,
                             uint32_t *out_speed_mbps) {
  if (port == NULL || out_up == NULL || out_speed_mbps == NULL) {
    return -EINVAL;
  }

  struct rte_eth_link link;
  memset(&link, 0, sizeof(link));
  rte_eth_link_get_nowait(port->port_id, &link);
  *out_up = link.link_status;
  *out_speed_mbps = link.link_speed;
  return 0;
}

uint16_t agave_dpdk_port_get_tx_queues(const struct agave_dpdk_port *port) {
  if (port == NULL) {
    return 0;
  }
  return port->tx_queues;
}

uint16_t agave_dpdk_rx_burst(const struct agave_dpdk_port *port, uint16_t queue_id,
                             struct rte_mbuf **rx_pkts, uint16_t nb_pkts) {
  if (port == NULL) {
    return 0;
  }
  return rte_eth_rx_burst(port->port_id, queue_id, rx_pkts, nb_pkts);
}

uint16_t agave_dpdk_tx_burst(const struct agave_dpdk_port *port, uint16_t queue_id,
                             struct rte_mbuf **tx_pkts, uint16_t nb_pkts) {
  if (port == NULL) {
    return 0;
  }
  return rte_eth_tx_burst(port->port_id, queue_id, tx_pkts, nb_pkts);
}

struct rte_mbuf *agave_dpdk_pktmbuf_alloc(const struct agave_dpdk_port *port) {
  if (port == NULL || port->mempool == NULL) {
    return NULL;
  }
  return rte_pktmbuf_alloc(port->mempool);
}

void agave_dpdk_pktmbuf_free(struct rte_mbuf *m) { rte_pktmbuf_free(m); }

void *agave_dpdk_pktmbuf_append(struct rte_mbuf *m, uint16_t len) {
  return rte_pktmbuf_append(m, len);
}

const uint8_t *agave_dpdk_pktmbuf_mtod(const struct rte_mbuf *m) {
  return rte_pktmbuf_mtod(m, const uint8_t *);
}

uint32_t agave_dpdk_pktmbuf_pkt_len(const struct rte_mbuf *m) { return rte_pktmbuf_pkt_len(m); }

int agave_dpdk_pktmbuf_is_contiguous(const struct rte_mbuf *m) {
  if (m == NULL) {
    return 0;
  }
  return rte_pktmbuf_is_contiguous(m);
}

int agave_dpdk_pktmbuf_linearize(struct rte_mbuf *m) {
  if (m == NULL) {
    return -EINVAL;
  }
  return rte_pktmbuf_linearize(m);
}
