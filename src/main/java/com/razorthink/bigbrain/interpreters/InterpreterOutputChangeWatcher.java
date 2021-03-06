package com.razorthink.bigbrain.interpreters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Watch the change for the development mode support
 */
public class InterpreterOutputChangeWatcher extends Thread {
    Logger logger = LoggerFactory.getLogger(InterpreterOutputChangeWatcher.class);

    private WatchService watcher;
    private final List<File> watchFiles = new LinkedList<File>();
    private final Map<WatchKey, File> watchKeys = new HashMap<WatchKey, File>();
    private InterpreterOutputChangeListener listener;
    private boolean stop;

    public InterpreterOutputChangeWatcher(InterpreterOutputChangeListener listener)
            throws IOException {
        watcher = FileSystems.getDefault().newWatchService();
        this.listener = listener;
    }

    public void watch(File file) throws IOException {
        String dirString;
        if (file.isFile()) {
            dirString = file.getParentFile().getAbsolutePath();
        } else {
            throw new IOException(file.getName() + " is not a file");
        }

        if (dirString == null) {
            dirString = "/";
        }

        Path dir = FileSystems.getDefault().getPath(dirString);
        logger.info("watch " + dir);
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        synchronized (watchKeys) {
            watchKeys.put(key, new File(dirString));
            watchFiles.add(file);
        }
    }

    public void clear() {
        synchronized (watchKeys) {
            for (WatchKey key : watchKeys.keySet()) {
                key.cancel();

            }
            watchKeys.clear();
            watchFiles.clear();
        }
    }

    public void shutdown() throws IOException {
        stop = true;
        clear();
        watcher.close();
    }

    public void run() {
        while (!stop) {
            WatchKey key = null;
            try {
                key = watcher.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException | ClosedWatchServiceException e) {
                break;
            }

            if (key == null) {
                continue;
            }
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == OVERFLOW) {
                    continue;
                }
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filename = ev.context();
                // search for filename
                synchronized (watchKeys) {
                    for (File f : watchFiles) {
                        if (f.getName().compareTo(filename.toString()) == 0) {
                            File changedFile;
                            if (filename.isAbsolute()) {
                                changedFile = new File(filename.toString());
                            } else {
                                changedFile = new File(watchKeys.get(key), filename.toString());
                            }
                            logger.info("File change detected " + changedFile.getAbsolutePath());
                            if (listener != null) {
                                listener.fileChanged(changedFile);
                            }
                        }
                    }
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                break;
            }
        }
    }
}

