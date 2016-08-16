# hadoop-mapreduce
Weather Data Report


public class WeatherData {
public static class QueryMappers extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
			String position = line.substring(10,25);
			String conditions = line.substring(47,50);
			context.write(new Text(position), new Text(conditions));
      }
}

 public static class QueryReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text favourable = new Text(" is favourable condition for game.");
	private Text notFavourable = new Text(" is unfavourable for game.");

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
	  int rain = 0;
	  int sunny =0;
	  int snow=0;

      for (Text val : values) 
	  {
		if (val.get()=="Rain"){
		rain=rain+1;
		}
       if (val.get()=="Sunny"){
		sunny=sunny+1;
		}
	   if (val.get()=="Snow"){
		snow=snow+1;
		}	  
      if(rain > sunny && rain > snow){
	  String newCondtion=rain;
	  }
	   if(sunny > rain && sunny > snow){
	  String newCondtion=sunny;
	  }
	   if(snow > sunny && snow > rain){
	  String newCondtion=snow;
	  }
	  if (newCondtion=="Sunny")
	  {
		context.write(key, favourable);
	  }
	  else {
	  context.write(key, notFavourable);
    }
  }
  }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Weather Data");
    job.setJarByClass(WeatherData.class);
    job.setMapperClass(QueryMappers.class);
    job.setCombinerClass(QueryReducer.class);
    job.setReducerClass(QueryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
