import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FraudDetectionJob {

    // One transaction: who, how much, when (time in milliseconds)
    public static class Transaction {
        public String customerId;
        public double amount;
        public long timestamp;

        public Transaction() {}
        public Transaction(String customerId, double amount, long timestamp) {
            this.customerId = customerId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }

    // Runs once for every transaction of a customer
    public static class FraudDetector
            extends KeyedProcessFunction<String, Transaction, String> {

        // STATE: time of this customer's last large transaction
        private transient ValueState<Long> lastLargeTime;

        @Override
        public void open(Configuration parameters) {
            lastLargeTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-large-time", Types.LONG));
        }

        @Override
        public void processElement(Transaction tx, Context ctx,
                                   Collector<String> out) throws Exception {
            if (tx.amount < 10000) {
                return;                              // small amount: ignore
            }

            Long last = lastLargeTime.value();       // read the state

            if (last != null && tx.timestamp - last <= 60000) {
                long gapSeconds = (tx.timestamp - last) / 1000;
                out.collect("FRAUD ALERT: Customer " + tx.customerId
                        + " made two large transactions within "
                        + gapSeconds + " seconds. Latest amount = " + tx.amount);
            }

            lastLargeTime.update(tx.timestamp);      // save the state
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Test data: customer, amount, time in milliseconds
        env.fromData(
                new Transaction("C101", 15000, 10000),    // 10 s
                new Transaction("C202", 5000, 20000),     // 20 s
                new Transaction("C101", 20000, 45000),    // 45 s
                new Transaction("C202", 12000, 50000),    // 50 s
                new Transaction("C101", 3000, 55000),     // 55 s
                new Transaction("C101", 18000, 90000),    // 90 s
                new Transaction("C202", 14000, 130000))   // 130 s
            .keyBy(tx -> tx.customerId)       // separate memory per customer
            .process(new FraudDetector())
            .print();

        env.execute("Fraud Detection Job");
    }
}
