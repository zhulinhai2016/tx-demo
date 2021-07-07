package com.tx.msg.demo.bank1.mqtx;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 14:45
 **/
public class MsgStorage {
    private List<DBDataSource> dbDataSources;
    private List<String> topicLists;

    public MsgStorage(List<DBDataSource> dbDataSources, List<String> topicLists) {
        this.dbDataSources = dbDataSources;
        this.topicLists = topicLists;
    }

    /**
     * 初始化定时任务，用来定时扫库
     *
     * @param config
     */
    public void init(Config config){
        // 定时扫库

    }
    public void close(){

    }

    public Map.Entry<Long,String> insertMsg(Connection con, String content, String topic, String tag) throws SQLException{
        String sql = "insert into tx_msg values(?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        int i = ps.executeUpdate();

        return null;
    }

    public MsgInfo getMsgById(Msg msg){

        return null;
    }

    public int updateMsgStatus(Msg msg){
        return 0;
    }
    public int updateMsgStatus(DataSource dataSrc,String id) throws SQLException{
        return 0;
    }
    public HashMap<String, DataSource> getDataSourcesMap(){
      return null;
    };

    public int deleteSendedMsg(DataSource dataSrc, int deleteMsgOneTimeNum) throws SQLException {
        return 0;
    }

    List<MsgInfo> getWaitingMsg(DataSource dataSrc, int limitNum) throws SQLException{
        return null;
    }
}
