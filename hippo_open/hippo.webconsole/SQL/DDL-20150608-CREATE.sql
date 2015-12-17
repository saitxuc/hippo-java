CREATE TABLE `hippo_cluster_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `clusterName` varchar(200) COLLATE utf8mb4_bin DEFAULT NULL,
  `details` varchar(500) COLLATE utf8mb4_bin DEFAULT NULL,
  `df` tinyint(4) DEFAULT NULL,
  `createDate` datetime DEFAULT NULL,
  `modifyDate` datetime DEFAULT NULL,
  `bucketLimit` int(11) DEFAULT NULL COMMENT '服务器的最大桶数',
  `copyCount` int(11) DEFAULT NULL COMMENT '备份数',
  `dbType` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '数据库类型',
  `replicatePort` int(11) DEFAULT NULL COMMENT '迁移端口号',
  `status` int(11) DEFAULT NULL COMMENT '状态 1未生效  2生效',
  `hashCount` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4

CREATE TABLE `hippo_server_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `server_id` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT 'IP和端口',
  `status` tinyint(4) DEFAULT NULL COMMENT '状态 1 已停用  2 启用中',
  `brokerName` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '服务器名称',
  `brokerVersion` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '服务器版本',
  `clusterName` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '集群名称',
  `clusterId` bigint(20) DEFAULT NULL COMMENT '集群id',
  `port` varchar(10) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '端口号',
  `bucketCount` int(11) DEFAULT NULL COMMENT '桶总数',
  `df` tinyint(4) DEFAULT NULL COMMENT '有效性 0 有效  1失效',
  `modifyDate` datetime DEFAULT NULL COMMENT '修改时间',
  `createDate` datetime DEFAULT NULL COMMENT '创建时间',
  `jmxPort` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=utf8mb4

CREATE TABLE `hippo_zk_cluster` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `clusterName` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
  `df` tinyint(4) DEFAULT NULL,
  `createdate` datetime DEFAULT NULL,
  `version` int(11) DEFAULT NULL,
  `config` varchar(200) COLLATE utf8mb4_bin DEFAULT NULL,
  `migration` varchar(2000) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3041 DEFAULT CHARSET=utf8mb4

CREATE TABLE `hippo_zk_dataservers` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `networkPort` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
  `content` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL,
  `df` tinyint(4) DEFAULT NULL,
  `createdate` datetime DEFAULT NULL,
  `zkClusterId` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2437 DEFAULT CHARSET=utf8mb4

CREATE TABLE `hippo_zk_tables` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `type` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
  `content` varchar(1000) COLLATE utf8mb4_bin DEFAULT NULL,
  `df` tinyint(4) DEFAULT NULL,
  `createdate` datetime DEFAULT NULL,
  `zkClusterId` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8781 DEFAULT CHARSET=utf8mb4



DROP TABLE IF EXISTS `hippo_cluster_info`;
DROP TABLE IF EXISTS `hippo_server_info`;
DROP TABLE IF EXISTS `hippo_zk_cluster`;
DROP TABLE IF EXISTS `hippo_zk_dataservers`;
DROP TABLE IF EXISTS `hippo_zk_tables`;