import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

public class MemoryWholeMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        JSONArray arr = new JSONArray(new Text(value.copyBytes()).toString());
        for(int i = 0 ; i < arr.length() ; ++i) {
            JSONObject obj = arr.getJSONObject(i);
            String id = obj.getString("id_str");

            JSONArray hashtags = obj.getJSONObject("entities").getJSONArray("hashtags");
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for(int j = 0 ; j < hashtags.length() ; ++j) {
                sb.append(hashtags.getJSONObject(j).getString("text"));
                sb.append(",");
            }
            if(sb.charAt(sb.length()-1) == ',') {
                sb.deleteCharAt(sb.length()-1);
            }
            sb.append("] [");
            sb.append(obj.getString("full_text").replace("\n",""));
            sb.append("]");

            context.write(new Text(id), new Text(sb.toString()));
        }
    }
}
