package com.dajiangtai.batch.dbus.source;
import com.dajiangtai.batch.dbus.model.Flow;
import com.dajiangtai.batch.dbus.utils.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
/**
 * 配置流
 *
 * @author dajiangtai
 * @create 2019-07-31-11:24
 */
@Slf4j
public class FlowSoure extends RichSourceFunction<Flow> {
    private static final long serialVersionUID = 3519222623348229907L;
    //状态位
    private volatile boolean isRunning = true;
    private String query = "select * from test.dbus_flow";
    private Flow flow=new Flow();

    @Override
    public void run(SourceContext<Flow> ctx) throws Exception {
        //定时读取数据库的flow表，生成FLow数据
        while (isRunning) {

            Connection conn=null;

            Statement stmt=null;

            ResultSet rs=null;

            try{
                conn= JdbcUtil.getConnection();

                stmt=conn.createStatement();

                rs=stmt.executeQuery(query);

                while (rs.next()) {
                    flow.setFlowId(rs.getInt("flowId"));
                    flow.setMode(rs.getInt("mode"));
                    flow.setDatabaseName(rs.getString("databaseName"));
                    flow.setTableName(rs.getString("tableName"));
                    flow.setHbaseTable(rs.getString("hbaseTable"));
                    flow.setFamily(rs.getString("family"));
                    flow.setUppercaseQualifier(rs.getBoolean("uppercaseQualifier"));
                    flow.setCommitBatch(rs.getInt("commitBatch"));
                    flow.setStatus(rs.getInt("status"));
                    flow.setRowKey(rs.getString("rowKey"));
                    log.info("load flow: "+flow.toString());
                    ctx.collect(flow);
                }
            }finally {
                JdbcUtil.close(rs,stmt,conn);
            }
            //隔一段时间读取，可以使用更新的配置生效
            Thread.sleep(60*1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}