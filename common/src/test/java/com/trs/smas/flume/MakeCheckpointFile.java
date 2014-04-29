package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.FileSystems;

public class MakeCheckpointFile {
	public static void main(String[] args) throws IOException{
		OffsetWatermark off = new OffsetWatermark("HYBASE_LOADTIME_A", "2014/04/26 20:54:32", 30);
		off.saveTo(FileSystems.getDefault().getPath("/Users/fengwei/Documents/TRS-workspace/checkpoint"));
	}
}
