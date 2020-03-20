import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

//NOTE: For now, only one reducer could be used
public class MemoryWholeReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, String> previousIDs = new HashMap<>();
    private DataOutputStream os;

    @Override
    protected void setup(Context context) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path pt = new Path("PreviousIDs/input.txt");
        if(!fs.exists(pt)) {
            os = fs.create(pt);
            return;
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        try {
            String line;
            line = br.readLine();
            while(line != null){
                int endOfKey = line.indexOf(' ');
                String key = line.substring(0, endOfKey);
                String tagsAndContent = line.substring(endOfKey+1);
                previousIDs.put(key, tagsAndContent);
                line = br.readLine();
            }
        } finally {
            br.close();
        }

        os = fs.append(pt); //for the reduce part
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
        if(previousIDs.containsKey(key.toString())) {
            previousIDs.remove(key.toString());
        }
        else {
            StringBuilder sb = new StringBuilder();
            sb.append(key.toString());
            sb.append(" ");
            sb.append(values.iterator().next());
            sb.append("\n");
            IOUtils.copyBytes(new ByteArrayInputStream(sb.toString().getBytes()), os, 4096, false);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        os.close();
        for(Map.Entry<String, String> entry : previousIDs.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(entry.getValue()));
        }
    }
}
