public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, String> {

    private Text word = new Text();

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, String> output,
                    Reporter reporter) throws IOException {
	String line = value.toString();
	StringTokenizer itr = new StringTokenizer(line);
	String key = itr.nextToken();
	word.set(key);
	output.collect(key,word);
	while (itr.hasMoreTokens()) {
	    word.set(itr.nextToken());
	    output.collect(key, word);
	}
    }
}