import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FetchData {
	private static final long MAX_RESULTS = 50;
	private static final int MAX_THREADS = 10;
	
	public static void main(String[] args) {
			long fileCount = 0; 
			
			ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);			
			
			String idFolder = "data/ids";
			File folder = new File(idFolder);       
			File[] files = folder.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					if (pathname.isFile() && pathname.getName().contains("txt"))
							return true;
					return false;
				}
			});     
			
			for(File file : files) {
				StringBuilder id = null;
				
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				int count = 0;
				String line = null;
				while ((line = br.readLine()) != null) {
					if (id == null) {
						id = new StringBuilder(line);
						count = 1;
					} else {
						id.append(",").append(line);
						count++;
					}

					if (count >= MAX_RESULTS || !br.ready()) {
						fileCount++;
						Runnable worker = new FetchAndSaveTask(id.toString(),fileCount);
						executor.execute(worker);
						System.out.println("Queued thread Number" + fileCount);
						id = null;
					}
				}
			} catch (IOException e) {
				    e.printStackTrace();
				}
			} 
	}
	
}
