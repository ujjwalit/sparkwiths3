package sparkwiths3;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsReader extends Configured implements Tool {
    
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    
    public int run(String[] args) throws Exception {
        
        if (args.length < 2) {
            System.err.println("HdfsReader [hdfs input path] [local output path]");
            return 1;
        }
        
        String localInputPath = args[0];
        Path inputPath = new Path(args[0]);
        String localOutputPath = args[1];
        Path outputPath = new Path(args[1]);
        Configuration conf = getConf();
        conf.set("fs.s3n.awsAccessKeyId","AKIAJ7P4BLMMFLRMKRRA");
        conf.set("fs.s3n.awsSecretAccessKey","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");
        conf.set("fs.defaultFS", "s3n://s3redujjwal");
        //conf.set("fs.s3n.bucket.logsdemocloud.endpoint", "s3.ap-south-1.amazonaws.com");
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        FileSystem fs = FileSystem.get(conf);
        /*if (fs.exists(outputPath)) {
            System.err.println("output path exists");
            return 1;
        }
        */
        FileStatus[] fileStatus = fs.listStatus(new Path("s3n://s3redujjwal/"));
        for(FileStatus status : fileStatus){
            System.out.println(status.getPath().toString());
        }
        //InputStream is = fs.open(inputPath);
        OutputStream os = fs.create(outputPath);
        fs.listFiles(outputPath, true);
        //OutputStream os = new BufferedOutputStream(new FileOutputStream(localOutputPath));
        InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
        //IOUtils.copyBytes(is, os, conf);
        return 0;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HdfsReader(), args);
        System.exit(returnCode);
    }
}