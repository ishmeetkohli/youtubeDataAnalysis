package fetcher;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoCategory;
import com.google.api.services.youtube.model.VideoCategoryListResponse;
import com.google.api.services.youtube.model.VideoListResponse;

class FetchAndSaveTask implements Runnable {
	private static YouTube youtube;
	private static HashMap<String, String> categoryMap;
	private static int errorCases = 0;
	private static final String API_KEY = "AIzaSyDO9y7ZsphMLvGdoHXXwVQuyq4drPz4J3E";

	static {
		try {
			youtube = new YouTube.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpRequestInitializer() {
				public void initialize(HttpRequest request) throws IOException {
				}
			}).setApplicationName("youtube-data-fetcher").build();

		} catch (Throwable t) {
			t.printStackTrace();
		}
		categoryMap = getCategories();
	}

	String ids;
	Long fileNumber;

	public FetchAndSaveTask(String ids, long fileNumber) {
		super();
		this.ids = ids;
		this.fileNumber = fileNumber;
	}

	@Override
	public void run() {
		try {

			YouTube.Videos.List videosListByIdRequest = youtube.videos().list("snippet,statistics");
			videosListByIdRequest.setId(ids);
			videosListByIdRequest.setKey(API_KEY);
			videosListByIdRequest.setFields("items(id,statistics/viewCount,statistics/likeCount,statistics/dislikeCount,statistics/commentCount,snippet/categoryId,snippet/publishedAt)");
			// videosListByIdRequest.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
			VideoListResponse searchResponse = videosListByIdRequest.execute();

			List<Video> searchResultList = searchResponse.getItems();

			if (searchResultList != null) {
				prettySave(searchResultList);
			}

		} catch (GoogleJsonResponseException e) {
			System.err.println("There was a service error: " + e.getDetails().getCode() + " : " + e.getDetails().getMessage());
		} catch (IOException e) {
			System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
		}

	}

	private void prettySave(List<Video> videoList) {

		if (videoList.isEmpty()) {
			System.out.println(" There aren't any results for your query.");
		}

		Path path = Paths.get("data/output/"+ fileNumber +".txt");

		// Use try-with-resource to get auto-closeable writer instance
		try (BufferedWriter writer = Files.newBufferedWriter(path)) {
			for (Video singleVideo : videoList) {
				String category = categoryMap.get(singleVideo.getSnippet().getCategoryId());
				if (category != null) {
					writer.write(singleVideo.getId() + "\t");
					writer.write(singleVideo.getStatistics().getViewCount() + "\t");
					writer.write(singleVideo.getStatistics().getLikeCount() + "\t");
					writer.write(singleVideo.getStatistics().getDislikeCount() + "\t");
					writer.write(singleVideo.getStatistics().getCommentCount() + "\t");
					writer.write(categoryMap.get(singleVideo.getSnippet().getCategoryId()) + "\t");
					writer.write(singleVideo.getSnippet().getPublishedAt() + "\n");
				} else {
					synchronized (FetchData.class) {
						errorCases++;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Done file number: " + fileNumber);
		System.out.println("Count : " + videoList.size());
		System.out.println("ErrorCases :" + errorCases);
		System.out.println("\n-------------------------------------------------------------\n");
	}

	static HashMap<String, String> getCategories() {
		YouTube.VideoCategories.List videoCategoriesListRequest;
		try {
			videoCategoriesListRequest = youtube.videoCategories().list("snippet");
			videoCategoriesListRequest.setKey(API_KEY);
			videoCategoriesListRequest.setRegionCode("US");
			VideoCategoryListResponse response = videoCategoriesListRequest.execute();
			List<VideoCategory> categoryList = response.getItems();
			HashMap<String, String> categoryMap = new HashMap<String, String>();

			categoryList.forEach(category -> {
				categoryMap.put(category.getId(), category.getSnippet().getTitle());
			});
			return categoryMap;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}