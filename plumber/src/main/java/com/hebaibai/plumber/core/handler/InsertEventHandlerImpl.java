package com.hebaibai.plumber.core.handler;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.hebaibai.plumber.core.EventHandler;
import com.hebaibai.plumber.core.SqlEventData;
import com.hebaibai.plumber.core.EventDataExecuter;
import com.hebaibai.plumber.core.utils.EventDataUtils;
import io.vertx.core.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 插入事件处理器
 *
 * @author hjx
 */
@Slf4j
public class InsertEventHandlerImpl extends AbstractEventHandler implements EventHandler {

    @Override
    public boolean support(EventHeader eventHeader, String dataBaseName, String tableName) {
        if (!status) {
            return false;
        }
        if (!EventType.isWrite(eventHeader.getEventType())) {
            return false;
        }
        return sourceDatabase.equals(dataBaseName) && sourceTable.equals(tableName);
    }

    /**
     * 修改 bug : 执行 insert into table values(111),(222);时 222 无法同步 2020年04月16日10:29:31
     *
     * @param eventBus
     * @param data
     */
    @Override
    public void handle(EventBus eventBus, EventData data) {
        String[][] insertRowsArray = EventDataUtils.getInsertRows(data);
        for (int x = 0; x < insertRowsArray.length; x++) {
            String[] rows = insertRowsArray[x];
            oneRow(rows);
        }
    }

    /**
     * 同步一行数据
     *
     * @param rows
     */
    void oneRow(String[] rows) {
        List<String> columns = sourceTableMateData.getColumns();
        Map<String, String> eventAfterData = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String sourceName = columns.get(i);
            String value = rows[i];
            if (!mapping.containsKey(sourceName)) {
                continue;
            }
            //目标字段
            String targetName = mapping.get(sourceName);
            if (targetName == null) {
                continue;
            }
            eventAfterData.put(targetName, value);
        }
        //填充插件数据
        SqlEventData eventPluginData = new SqlEventData(SqlEventData.TYPE_INSERT);
        //添加变动后的数据
        eventPluginData.setAfter(eventAfterData);
        eventPluginData.setSourceDatabase(this.sourceDatabase);
        eventPluginData.setSourceTable(this.sourceTable);
        eventPluginData.setTargetDatabase(this.targetDatabase);
        eventPluginData.setTargetTable(this.targetTable);
        eventPluginData.setKey(ids());
        for (EventDataExecuter eventPlugin : eventPlugins) {
            try {
                eventPlugin.execute(eventPluginData);
            } catch (Exception e) {
                log.error(eventPlugin.getClass().getName(), e);
            }
        }
    }

    @Override
    public String getName() {
        return "insert event hander";
    }
}
