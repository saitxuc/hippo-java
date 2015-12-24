CREATE TABLE `hippo_cluster_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `clusterName` varchar(200) COLLATE utf8mb4_bin DEFAULT NULL,
  `details` varchar(500) COLLATE utf8mb4_bin DEFAULT NULL,
  `df` tinyint(4) DEFAULT NULL,
  `createDate` datetime DEFAULT NULL,
  `modifyDate` datetime DEFAULT NULL,
  `bucketLimit` int(11) DEFAULT NULL COMMENT '�����������Ͱ��',
  `copyCount` int(11) DEFAULT NULL COMMENT '������',
  `dbType` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '���ݿ�����',
  `replicatePort` int(11) DEFAULT NULL COMMENT 'Ǩ�ƶ˿ں�',
  `status` int(11) DEFAULT NULL COMMENT '״̬ 1δ��Ч  2��Ч',
  `hashCount` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4

CREATE TABLE `hippo_server_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '����',
  `server_id` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT 'IP�Ͷ˿�',
  `status` tinyint(4) DEFAULT NULL COMMENT '״̬ 1 ��ͣ��  2 ������',
  `brokerName` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '����������',
  `brokerVersion` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '�������汾',
  `clusterName` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '��Ⱥ����',
  `clusterId` bigint(20) DEFAULT NULL COMMENT '��Ⱥid',
  `port` varchar(10) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '�˿ں�',
  `bucketCount` int(11) DEFAULT NULL COMMENT 'Ͱ����',
  `df` tinyint(4) DEFAULT NULL COMMENT '��Ч�� 0 ��Ч  1ʧЧ',
  `modifyDate` datetime DEFAULT NULL COMMENT '�޸�ʱ��',
  `createDate` datetime DEFAULT NULL COMMENT '����ʱ��',
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