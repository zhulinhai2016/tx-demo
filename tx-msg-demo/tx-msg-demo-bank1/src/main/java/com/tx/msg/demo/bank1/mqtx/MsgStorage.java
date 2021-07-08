package com.tx.msg.demo.bank1.mqtx;

import com.tx.msg.demo.bank1.data.pool.DbPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 14:45
 **/
public class MsgStorage {
    private DbPool dbDataSources;
    private List<String> topicLists;

    public MsgStorage(DbPool dbDataSources, List<String> topicLists) {
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

    public int insertMsg(Connection con, String content, String topic, String tag) throws SQLException{
        String sql = "insert into tx_message (content,topic,tag,status,create_time)  values(?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1,content);
        ps.setString(2,topic);
        ps.setString(3,tag);
        ps.setInt(4,0);
//        ps.setDate(5, (java.sql.Date) new Date());
        ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
        int i = ps.executeUpdate();
        return i;
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
