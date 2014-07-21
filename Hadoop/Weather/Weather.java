public void run(String inputPath, String outputPath) throws Exception {
    JobConf conf = new JobConf(Weather.class);
    conf.setJobName("weatherdata");

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(String.class);

    conf.setMapperClass(MapWeather.class);
    conf.setCombinerClass(MapWeather.class);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    JobClient.runJob(conf);
}