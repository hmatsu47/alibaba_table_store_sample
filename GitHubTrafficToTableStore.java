package alisample;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZonedDateTime;

import org.json.JSONArray;
import org.json.JSONObject;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyOption;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.ReturnType;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.StreamRequestHandler;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;

public class GitHubTrafficToTableStore implements StreamRequestHandler {

	private static final String GITHUB_ENDPOINT =	"https://api.github.com/repos";
	private static final String GITHUB_USER =		"【GitHub User】";
	private static final String GITHUB_REPO =		"【GitHub Repository】";
	private static final String GITHUB_TOKEN =		"【GitHub Token】";

	private static final String TS_ENDPOINT =		"【Table Store Endpoint】";
	private static final String TS_INSTANCE =		"【Table Store Instance】";
	private static final String TS_TABLENAME =		"【Table Store Table Name】";
	private static final String TS_ACCESSKEY_ID =	"【Access Key ID】";
	private static final String TS_ACCESSKEY_SEC =	"【Access Key Secret】";
	private static final String TS_PT_KEY =			"date";
	private static final String TS_AI_KEY =			"ai";

    @Override
    public void handleRequest(
            InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

    	SyncClient client = new SyncClient(TS_ENDPOINT, TS_ACCESSKEY_ID, TS_ACCESSKEY_SEC, TS_INSTANCE);

    	try {
    		int minusDays = 1;
    		// Tableがなければ作成
    		if(!isTableExist(client)) {
    			createTable(client);
    			minusDays = 16;
    		}
    		// 対象範囲
    		final ZonedDateTime toDate = ZonedDateTime.now().minusHours(23);
    		final ZonedDateTime fromDate = toDate.minusDays(minusDays);
    		// GitHubからデータを取得→あればTable Storeへ書き込み
    		HttpResponse<JsonNode> response = getGitHubResponse();
    		if(response.getStatus() == 200) {
    			JSONArray views = response.getBody().getObject().getJSONArray("views");
    			views.forEach(obj -> {
					try {
						final ZonedDateTime curDate = ZonedDateTime.parse(((JSONObject) obj).getString("timestamp"));
						// 対象範囲のデータのみ書き込み
						if(curDate.isAfter(fromDate) && toDate.isAfter(curDate)) {
							putTable(client, ((JSONObject) obj).getLong("count"), ((JSONObject) obj).getLong("uniques"), ((JSONObject) obj).getString("timestamp"));
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
    			});
    		} else {
    			System.out.println("viewsを取得できませんでした。");
    		}
    	} catch(Exception e) {
    		e.printStackTrace();
    	} finally {
            client.shutdown();
            outputStream.write(new String("end.").getBytes());
    	}
    }

    // GitHub読み取り
    private static HttpResponse<JsonNode> getGitHubResponse() throws Exception {
    	final String url =		GITHUB_ENDPOINT + "/" + GITHUB_USER + "/" + GITHUB_REPO + "/traffic/views";
    	final String token =	"token " + GITHUB_TOKEN;
    	return Unirest.get(url)
    					.header("Authorization", token)
    					.header("accept", "application/json")
    					.asJson();
    }

    // Table Store書き込み
	private static PrimaryKey putTable(SyncClient client, long count, long uniques, String timeStamp) throws Exception {
		PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(TS_PT_KEY, PrimaryKeyValue.fromString(timeStamp));
        primaryKeyBuilder.addPrimaryKeyColumn(TS_AI_KEY, PrimaryKeyValue.AUTO_INCREMENT);
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TS_TABLENAME, primaryKey);
        rowPutChange.setReturnType(ReturnType.RT_PK);

        rowPutChange.addColumn(new Column("Count", ColumnValue.fromLong(count)));
        rowPutChange.addColumn(new Column("Uniques", ColumnValue.fromLong(uniques)));

        PutRowResponse response = client.putRow(new PutRowRequest(rowPutChange));

        PrimaryKey pk = response.getRow().getPrimaryKey();
        System.out.println("PrimaryKey:" + pk.toString());

        return pk;
	}

	// Table存在チェック
    private static boolean isTableExist(SyncClient client) {
        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(TS_TABLENAME);

        try {
            client.describeTable(request);
        } catch(TableStoreException e) {
            if(e.getErrorCode().equals("OTSObjectNotExist")) {
                return false;
            }
            throw e;
        }
        return true;
    }

	// Table作成
    private static void createTable(SyncClient client) throws Exception {
        TableMeta tableMeta = new TableMeta(TS_TABLENAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TS_PT_KEY, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TS_AI_KEY, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));

        int timeToLive = -1;
        int maxVersions = 1;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        client.createTable(request);
    }

}
