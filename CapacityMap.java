package maps.MapCapacity;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MapCapacity<K, V> extends ConcurrentHashMap<K, V> implements Closeable{

	private static final long serialVersionUID = 4173482847741850625L;
	private static long size = 0L;
	private static int pieceSize;
	private static int currentPieceSize;
	private ConcurrentHashMap<K, V> currentPiece;
	private Store<K, V> store;
	private static final int PIECE_SIZE = 100;
	
	
	public MapCapacity() {
		super(PIECE_SIZE);
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
			@Override
			public void run() {
				try {
					close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				clear();
			}}));
	}

	@Override
	public void close() throws IOException {
		if (store != null) {
			store.close();
			store = null;
			currentPiece = null;
		}
	}
	
	/**
	 * Default size() implementation which down casts the real size (long) to an int for interface compatibility.
	 * This number is unreliable for very large collections.
	 */
	@Override
	public int size() {
		return (int) size;
	}
	
	public int getPieceSize() {
		return this.pieceSize;
	}
	
	/**
	 * This method is useful if the collection is very large because then the default size() implementation down casts to an int and looses information.
	 * 
	 * @return The real size of this collection as long
	 */
	public long getRealSize() {
		return size;
	}
	
	/**
	 * Flush writes the current piece to disk and creates a new, empty piece.
	 */
	public void flush() {
		if(!currentPiece.isEmpty()) {
			store.write(currentPiece);
			currentPiece = new ConcurrentHashMap<>(pieceSize);
			currentPieceSize = 0;
		}
	}
	
	@Override
	public V put(K key, V value) {
		V v = super.put(key, value);
		checkEntry(v);
		return v;
	}
	
	@Override
	public V putIfAbsent(K key, V value) {
		V v = super.putIfAbsent(key, value);
		checkEntry(v);
		return v;
	}
	
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		int sizeInc = super.size();
		super.putAll(m);
		sizeInc = super.size() - sizeInc;
		if (sizeInc > 0){
			currentPieceSize += sizeInc;
			if(currentPieceSize >= pieceSize) {
				flush();
			}
			size += sizeInc;
		}
	}

	private void checkEntry(V v) {
		if (v == null){
			currentPieceSize++;
			if(currentPieceSize >= pieceSize) {
				flush();
			}
			size++;
		}
	}
	
	private static class Store<K, V> implements Closeable {
		private final File file;
		private final ObjectOutputStream outputStream;

		Store() {
			try {
				file = createTmpFileForCHCMap();
				outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
				outputStream.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private File createTmpFileForCHCMap() {
			String tmpDir = System.getProperty("java.io.tmpdir");
			String fileNamePrefix = "MapCapacity-";
			String fileNamePostfix =  UUID.randomUUID().toString();
			String extension = ".bin";
			File file = new File(tmpDir, fileNamePrefix + fileNamePostfix + extension);
			file.deleteOnExit();
			return file;
		}
		
		@Override
		public void close() {
			try {
				outputStream.close();
				file.delete();
			} catch(IOException ioe) {}
		}

		void write(ConcurrentHashMap<K, V> chunk) {
			try {
				outputStream.writeObject(chunk);
				outputStream.flush();
				outputStream.reset();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		PieceReader<K, V> getReader() {
			try {
				PieceReader<K,V> reader = new PieceReader<>(new ObjectInputStream(new BufferedInputStream(new FileInputStream(file))));
				return reader;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static class PieceReader<K, V> implements Closeable {
		private ObjectInputStream inputStream;

		PieceReader(ObjectInputStream inputStream) {
			this.inputStream = inputStream;
		}

		Optional<ConcurrentHashMap<K, V>> readPiece() {
			try {
				@SuppressWarnings("unchecked")
				ConcurrentHashMap<K, V> piece = (ConcurrentHashMap<K, V>) inputStream.readObject();
				return Optional.of(piece);
			} catch (EOFException e) {
				return Optional.empty();
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() throws IOException {
			inputStream.close();
		}
	}
}
