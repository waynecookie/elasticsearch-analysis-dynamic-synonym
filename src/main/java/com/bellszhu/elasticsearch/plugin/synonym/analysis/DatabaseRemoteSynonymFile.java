package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import com.bellszhu.elasticsearch.plugin.DynamicSynonymPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.env.Environment;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

/**
 * 加载DB中同义词
 * DB当前是基于mysql,可以是mysql、postgresql，根据实际情况定义
 * mysql: com.mysql.jdbc.Driver
 * mysql8: com.mysql.cj.jdbc.Driver
 * postgresql: org.postgresql.Driver
 *
 * @author wayne
 */
public class DatabaseRemoteSynonymFile implements SynonymFile {

    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    private final String format;

    private final boolean expand;

    private final boolean lenient;

    private final Analyzer analyzer;

    private final Environment env;

    /**
     * 数据库配置
     * 类似：/home/elastic/elastic.7.8/elasticsearch-7.8.1/config/fromDatabase
     */
    private final String location;
    /**
     * 数据库配置⽂件名
     */
    private final static String JDBC_RELOAD_PROPERTIES = "jdbc-reload.properties";

    /**
     * 数据库地址
     */
    private static final String JDBC_URL = "jdbc.url";
    /**
     * 数据库⽤⼾名
     */
    private static final String JDBC_USER = "jdbc.user";
    /**
     * 数据库密码
     */
    private static final String JDBC_PASSWORD = "jdbc.password";
    /**
     * 数据库驱动类
     */
    private static final String JDBC_CLASS_NAME = "jdbc.className";
    /**
     * 数据库查询同义词sql语句
     */
    private static final String JDBC_RELOAD_SYNONYM_SQL = "jdbc.reload.synonym.sql";
    /**
     * 查询数据库同义词在数据库版本号
     */
    private static final String JDBC_RELOAD_SYNONYM_VERSION = "jdbc.reload.synonym.version";

    private static final String SYNONYM_WORDS = "words";
    private static final String SYNONYM_VERSION = "version";

    /**
     * 默认 fromDatabase，可以自定义
     */
    static String SYNONYMS_PATH = "fromDatabase";

    /**
     * 当前节点的同义词版本号
     */
    private long thisSynonymVersion = -1L;

    private final Connection connection = null;
    private Statement statement = null;
    private final Properties props;
    private final Path conf_dir;

    DatabaseRemoteSynonymFile(Environment env, Analyzer analyzer, boolean expand, boolean lenient, String format, String location) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.format = format;
        this.lenient = lenient;
        this.env = env;
        this.location = location;
        this.props = new Properties();

        //读取当前 jar 包存放的路径
        Path filePath =
                PathUtils.get(
                        new File(DynamicSynonymPlugin.class.getProtectionDomain().getCodeSource()
                                .getLocation().getPath())
                                .getParent(), "config")
                        .toAbsolutePath();
        this.conf_dir = filePath.resolve(JDBC_RELOAD_PROPERTIES);

        //判断⽂件是否存在
        File configFile = conf_dir.toFile();
        InputStream input = null;
        try {
            input = new FileInputStream(configFile);
        } catch (FileNotFoundException e) {
            logger.info("数据库配置⽂件 jdbc-reload.properties 没有找到， " + e);
        }
        if (input != null) {
            try {
                props.load(input);
                SYNONYMS_PATH = props.getOrDefault("synonyms.path", SYNONYMS_PATH).toString();
            } catch (IOException e) {
                logger.error("数据库配置⽂件 jdbc-reload.properties 加载失败，" + e);
            }
        }
        isNeedReloadSynonymMap();
    }

    /**
     * 加载同义词词典⾄SynonymMap中
     *
     * @return SynonymMap
     */
    @Override
    public SynonymMap reloadSynonymMap() {
        try {
            logger.info("开始加载 database 同义词 {}.", location);
            Reader rulesReader = getReader();
            SynonymMap.Builder parser = RemoteSynonymFile.getSynonymParser(rulesReader, format, expand, lenient, analyzer);
            return parser.build();
        } catch (Exception e) {
            logger.error("加载 database 同义词文件失败 {} !", e, location);
            throw new IllegalArgumentException("无法重新加载 database 同义词文件以生成同义词", e);
        }
    }

    /**
     * 判断是否需要进⾏重新加载
     *
     * @return true or false
     */
    @Override
    public boolean isNeedReloadSynonymMap() {
        try {
            Long lastVersion = getLastVersion();
            if (thisSynonymVersion < lastVersion) {
                thisSynonymVersion = lastVersion;
                return true;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return false;
    }

    /**
     * 同义词库的加载
     *
     * @return Reader
     */
    @Override
    public Reader getReader() {
        StringBuffer sb = new StringBuffer();
        try {
            ArrayList<String> synonymData = loadDatabaseSynonymDict();
            for (int i = 0; i < synonymData.size(); i++) {
                logger.info("遍历同义词: {}", synonymData.get(i));
                // 获取⼀⾏⼀⾏的记录，每⼀条记录都包含多个词，形成⼀个词组，词与词之间使⽤英⽂逗号分割
                sb.append(synonymData.get(i)).append(System.getProperty("line.separator"));
            }
        } catch (Exception e) {
            logger.error("加载同义词失败");
        }
        return new StringReader(sb.toString());
    }

    /**
     * 获取 database 中同义词版本号信息
     * ⽤于判断同义词是否需要进⾏重新加载
     *
     * @return getLastVersion
     */
    public Long getLastVersion() {
        ResultSet resultSet = null;
        Long lastVersion = 0L;
        try {
            if (statement == null) {
                statement = getStatement(props, connection);
            }
            resultSet = statement.executeQuery(props.getProperty(JDBC_RELOAD_SYNONYM_VERSION));
            while (resultSet.next()) {
                lastVersion = resultSet.getLong(SYNONYM_VERSION);
                logger.info("当前 database 同义词版本号为: {}, 当前节点同义词库版本号为: {}", lastVersion, thisSynonymVersion);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeResultSet(resultSet);
        }
        return lastVersion;
    }

    /**
     * 查询数据库中的同义词
     *
     * @return DBData
     */
    public ArrayList<String> loadDatabaseSynonymDict() {
        ArrayList<String> arrayList = new ArrayList<>();
        ResultSet resultSet = null;
        try {
            if (statement == null) {
                statement = getStatement(props, connection);
            }
            logger.info("正在查询同义词,sql: {}", props.getProperty(JDBC_RELOAD_SYNONYM_SQL));
            resultSet = statement.executeQuery(props.getProperty(JDBC_RELOAD_SYNONYM_SQL));
            while (resultSet.next()) {
                String theWord = resultSet.getString(SYNONYM_WORDS);
                arrayList.add(theWord);
            }
        } catch (SQLException e) {
            logger.error(e);
        } finally {
            closeResultSet(resultSet);
        }
        return arrayList;
    }

    /**
     * 获取数据库可执⾏连接
     *
     * @param props
     * @param conn
     * @throws SQLException
     */
    private static Statement getStatement(Properties props, Connection conn)
            throws SQLException {
        conn = DriverManager.getConnection(
                props.getProperty(JDBC_URL),
                props.getProperty(JDBC_USER),
                props.getProperty(JDBC_PASSWORD));
        return conn.createStatement();
    }

    /**
     * 释放资源
     *
     * @param conn
     * @param st
     * @param rs
     */
    public static void colseResource(Connection conn, Statement st, ResultSet rs) {
        closeResultSet(rs);
        closeStatement(st);
        closeConnection(conn);
    }

    /**
     * 释放连接 Connection
     *
     * @param conn
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //等待垃圾回收
        conn = null;
    }

    /**
     * 释放语句执行者 Statement
     *
     * @param st
     */
    public static void closeStatement(Statement st) {
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //等待垃圾回收
        st = null;
    }

    /**
     * 释放结果集 ResultSet
     *
     * @param rs
     */
    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //等待垃圾回收
        rs = null;
    }

}
