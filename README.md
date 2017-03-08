# rocketmq-storm [![Build Status](https://travis-ci.org/rocketmq/rocketmq-storm.svg?branch=master)](https://travis-ci.org/rocketmq/rocketmq-storm)

## Description

rocketmq-storm allows a Storm topology to consume an RocketMQ queue as an input source. It currently provides:

#### SimpleMessageSpout: 
An simple implementation of org.apache.storm.topology.IRichSpout,consumes the messages one by one.full features spout implementation exception flow control function.

#### BatchMessageSpout: 
As the name implies,It handle the messages in a batch way,also with supporting reliable messages.

#### StreamMessageSpout: 
Based on batchMessageSpout,cache batch messages and emit message one by one.It is also recommendation spout at the present stage.

#### RocketMQTridentSpout: 
Based on latest trident api.

#### SimplePullMessageSpout: 
Support message pulling model.

## Documentation
Please look forward to!

## Code Snippet,see [here](https://github.com/rocketmq/rocketmq-storm/blob/master/src/main/java/org/apache/rocketmq/integration/storm/topology/SimpleTopology.java)

#### 
    private static final String BOLT_NAME      = "notifier";
    private static final String PROP_FILE_NAME = "mqspout.test.prop";

    private static Config       conf           = new Config();
    private static boolean      isLocalMode    = true;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = buildTopology(ConfigUtils.init(PROP_FILE_NAME));

        submitTopology(builder);
    }

    private static TopologyBuilder buildTopology(Config config) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        int boltParallel = NumberUtils.toInt((String) config.get("topology.bolt.parallel"), 1);

        int spoutParallel = NumberUtils.toInt((String) config.get("topology.spout.parallel"), 1);

        BoltDeclarer writerBolt = builder.setBolt(BOLT_NAME, new RocketMqBolt(), boltParallel);

        StreamMessageSpout defaultSpout = (StreamMessageSpout) RocketMQSpoutFactory
                .getSpout(Spouts.STREAM.getValue());
        RocketMQConfig mqConig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);
        defaultSpout.setConfig(mqConig);

        String id = (String) config.get(ConfigUtils.CONFIG_TOPIC);
        builder.setSpout(id, defaultSpout, spoutParallel);

        writerBolt.shuffleGrouping(id);
        return builder;
    }

    private static void submitTopology(TopologyBuilder builder) {
        try {
            String topologyName = String.valueOf(conf.get("topology.name"));
            StormTopology topology = builder.createTopology();

            if (isLocalMode == true) {
                LocalCluster cluster = new LocalCluster();
                conf.put(Config.STORM_CLUSTER_MODE, "local");

                cluster.submitTopology(topologyName, conf, topology);

                Thread.sleep(50000);

                cluster.killTopology(topologyName);
                cluster.shutdown();
            } else {
                conf.put(Config.STORM_CLUSTER_MODE, "distributed");
                StormSubmitter.submitTopology(topologyName, conf, topology);
            }

        } catch (AlreadyAliveException e) {
            LOG.error(e.getMessage(), e.getCause());
        } catch (InvalidTopologyException e) {
            LOG.error(e.getMessage(), e.getCause());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e.getCause());
        }
    } 


## How to upload task jar
To produce a jar:

$ mvn clean install


Run it

$cd target && storm jar rocketmq-storm-1.1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.rocketmq.integration.storm.topology.SimpleTopology


## Compatibility
#### [Apache RocketMQ 4.x](https://github.com/apache/incubator-rocketmq)

#### [Apache Storm 1.0.x](https://github.com/apache/storm)
