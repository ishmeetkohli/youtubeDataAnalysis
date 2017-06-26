package fetcher;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JoinFiles {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File folder = new File("data/videos");
		OutputStream out;
		try {
			out = new FileOutputStream("merged/mergedFile.txt");
			File[] fileList = folder.listFiles();
			long count = 0;
			long total = fileList.length;
			byte[] buf = new byte[1024];
			for (File file : fileList) {
				System.out.println();
				InputStream in = new FileInputStream(file);
				int b = 0;
				while ((b = in.read(buf)) >= 0) {
					out.write(buf, 0, b);
					out.flush();
				}
				in.close();
				count++;
				System.out.println("\nPercentage : " + count/(float)total);
			}
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
