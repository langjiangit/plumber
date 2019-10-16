package com.hebaibai.plumber.core.handler;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.hebaibai.plumber.ConsumerAddress;
import com.hebaibai.plumber.core.handler.plugin.EventPlugin;
import com.hebaibai.plumber.core.handler.plugin.EventPluginData;
import com.hebaibai.plumber.core.utils.EventDataUtils;
import io.vertx.core.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 删除事件处理器
 *
 * @author hjx
 */
@Slf4j
public class DeleteEventHandlerImpl extends AbstractEventHandler implements EventHandler {

    @Override
    public boolean support(EventHeader eventHeader, String dataBaseName, String tableName) {
        if (!status) {
            return false;
        }
        if (!EventType.isDelete(eventHeader.getEventType())) {
            return false;
        }
        return sourceDatabase.equals(dataBaseName) && sourceTable.equals(tableName);
    }

    @Override
    public void handle(EventBus eventBus, EventData data) {
        String[] rows = EventDataUtils.getDeleteRows(data);
        List<String> columns = sourceTableMateData.getColumns();
        Map<String, String> eventAfterData = new HashMap<>();
        List<String> wheres = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String sourceName = columns.get(i);
            String value = rows[i];
            boolean isKey = key.equals(sourceName) && mapping.containsKey(sourceName);
            String targetName = mapping.get(sourceName);
            eventAfterData.put(targetName, value);
            if (!isKey) {
                continue;
            }
            if (value == null) {
                wheres.add(targetName + " = null ");
            } else {
                wheres.add("`" + targetName + "`" + " = '" + value + "' ");
            }
        }
        //填充插件数据
        EventPluginData eventPluginData = new EventPluginData(EventPluginData.TYPE_DELETE);
        //添加变动后的数据
        eventPluginData.setAfter(eventAfterData);
        eventPluginData.setSourceDatabase(this.sourceDatabase);
        eventPluginData.setSourceTable(this.sourceTable);
        eventPluginData.setTargetDatabase(this.targetDatabase);
        eventPluginData.setTargetTable(this.targetTable);
        eventPluginData.setKey(mapping.get(this.key));
        for (EventPlugin eventPlugin : eventPlugins) {
            try {
                eventPlugin.doWithPlugin(eventPluginData);
            } catch (Exception e) {
                log.error(eventPlugin.getClass().getName(), e);
            }
        }
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM ");
        sqlBuilder.append(targetDatabase).append(".").append(targetTable);
        sqlBuilder.append(" WHERE ");
        sqlBuilder.append(String.join("and ", wheres));
        String sql = sqlBuilder.toString();
        eventBus.send(ConsumerAddress.EXECUTE_SQL_DELETE, sql);
    }

    @Override
    public String getName() {
        return "delete event hander";
    }
}
