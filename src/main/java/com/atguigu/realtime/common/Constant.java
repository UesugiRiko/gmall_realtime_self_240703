package com.atguigu.realtime.common;

public class Constant {
    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String MYSQL_HOSTNAME_GMALL = "hadoop102";
    public static final int MYSQL_PORT_GMALL = 3306;
    public static final String MYSQL_USERNAME_GMALL = "root";
    public static final String MYSQL_PASSWORD_GMALL = "000000";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_UV_DETAIL = "dwd_traffic_uv_detail";
    public static final String TOPIC_DWD_TRAFFIC_UJ_DETAIL = "dwd_traffic_uj_detail";
}
