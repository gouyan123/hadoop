package cn.edu360.hdfs.datacollect;

import java.util.Timer;

public class DataCollectMain {
	public static void main(String[] args) {
		/**Timer定时器*/
		/**timer.schedule(TimerTask task,long delay,long period) 周期执行线程任务task，period为时间周期*/
		/**timer.schedule(new TimerTask(){
							@Override
							public void run() {
								...
							}
		},delay,period)*/
		Timer timer = new Timer();
		timer.schedule(new CollectTask(), 0, 60*60*1000L);
		timer.schedule(new BackupCleanTask(), 0, 60*60*1000L);
	}
}
