package com.hebaibai.plumber.core;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;

/**
 * @author hjx
 */
public interface EventHandler {

    void setAuth(Auth auth);

    /**
     * 是否支持当前操作
     *
     * @param eventType
     * @param dataBaseName
     * @param tableName
     * @return
     */
    boolean support(EventType eventType, String dataBaseName, String tableName);


    /**
     * 设置状态
     *
     * @param isRun true:可用
     * @param isRun false:不可用
     * @return
     */
    void setStatus(boolean isRun);

    /**
     * 处理类
     *
     * @param data
     */
    void handle(EventData data);

    /**
     * 获取名称
     *
     * @return
     */
    String getName();

}
