package cn.edu360.hdfs.datacollect;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;

public class BackupCleanTask extends TimerTask {
	@Override
	public void run() {
		/*探测备份目录*/
		/*判断备份日期子目录是否超过24h，超过则删除*/
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		long now = new Date().getTime();
		try {
			// 探测本地备份目录
			File backupBaseDir = new File("d:/logs/backup/");
			File[] dayBackDir = backupBaseDir.listFiles();
			// 判断备份日期子目录是否已超24小时
			for (File dir : dayBackDir) {
				long time = sdf.parse(dir.getName()).getTime();
				if(now-time>24*60*60*1000L){
					FileUtils.deleteDirectory(dir);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
