/**
 * Title:		flume-elasticsearch
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.io.File;
import java.util.ArrayList;

import org.apache.commons.lang.SerializationUtils;

/**
 * 离散水位线,用于导数据时标记文件位置和文件start positon
 * 
 * @since selamat
 * 
 */
public class DiscreteWatermark implements Serializable {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private ArrayList<File> fileList;
	private long startPosition = 0L;
	private File currentFile;

	public DiscreteWatermark(ArrayList<File> fileList, File currentFile, long startPosition) {
		this.fileList = fileList;
		this.currentFile = currentFile;
		this.startPosition = startPosition;
	}


	public void rise(File currentFile, long startPosition) {
		this.currentFile = currentFile;
		this.startPosition = startPosition;
	}

	public void add(File file) {
		this.fileList.add(file);
	}

	/** 
	 * 从文件加载watermark
	 * @param path
	 * @return 文件不存在时返回null
	 * @throws IOException
	 * @since selamat
	*/
	public static DiscreteWatermark loadFrom(Path path) throws IOException{
		if(!Files.exists(path)){
			return null;
		}
		return (DiscreteWatermark)SerializationUtils.deserialize(Files.readAllBytes(path));
	}
	
	public void saveTo(Path path) throws IOException{
		Files.write(path, SerializationUtils.serialize(this), StandardOpenOption.CREATE);
	}
	
	public ArrayList<File> getFileList() {
		return fileList;
	}


	public void setFileList(ArrayList<File> fileList) {
		this.fileList = fileList;
	}


	public long getStartPosition() {
		return startPosition;
	}


	public void setStartPosition(long startPosition) {
		this.startPosition = startPosition;
	}


	public File getCurrentFile() {
		return currentFile;
	}


	public void setCurrentFile(File currentFile) {
		this.currentFile = currentFile;
	}
	
	/*
	public String toString() {
		return new ToStringBuilder(this).append("applyTo", getApplyTo())
				.append("cursor", getCursor()).append("offset", offset)
				.toString();
	} */
}
