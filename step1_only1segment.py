"""
This script is used to create only single segment for each youtube ID in order to detect the language type.
The idea is to avoid lot of segment downloads. 
"""


# Importing the libraries
import pandas as pd
import os

import sys
import os
from multiprocessing.pool import ThreadPool

from yt_dlp import YoutubeDL
import ffmpeg
from tqdm import tqdm


# ============== Helper functions ==============
class VidInfo:
    def __init__(self, yt_id, start_time, end_time, outdir):
        self.yt_id = yt_id
        self.start_time = float(start_time)
        self.end_time = float(end_time)
        # self.out_filename = os.path.join(outdir, yt_id + '_' + start_time + '_' + end_time + '.wav')
        self.out_filename = os.path.join(outdir, yt_id + '.wav')
        

def download(vidinfo):

    yt_base_url = 'https://www.youtube.com/watch?v='

    yt_url = yt_base_url+vidinfo.yt_id

    ydl_opts = {
        "format": "22/18",
        "quiet": True,
        "ignoreerrors": True,
        "no_warnings": True,
        "sub-lang": "en",
        "write-auto-sub": True,
        "convert-subs": "srt",
        "skip-download": True,
        "write-sub": True,
    }
    try:
        with YoutubeDL(ydl_opts) as ydl:
            download_url = ydl.extract_info(url=yt_url, download=False)['url']
    except:
        return_msg = '{}, ERROR (youtube)!'.format(vidinfo.yt_id)
        return return_msg
    try:
        (
            ffmpeg
                .input(download_url, ss=vidinfo.start_time, to=vidinfo.end_time)
                .output(vidinfo.out_filename, format='wav', 
                        acodec='pcm_s16le', audio_bitrate=128000,
                        strict='experimental',
                        **ydl_opts)
                .global_args('-y')
                .global_args('-loglevel', 'error')
                .run()

        )
    except:
        return_msg = '{}, ERROR (ffmpeg)!'.format(vidinfo.yt_id)
        return return_msg

    return '{}, DONE!'.format(vidinfo.yt_id)


# ================ Main Code =======================
split = "test"
CSV_FILE = f"avspeech_{split}.csv"
OUT_DIR = f"step1_segment/{split}"

os.makedirs(OUT_DIR, exist_ok=True)

# Extracting single segment audio
df = pd.read_csv(CSV_FILE, names = ["id", "start","end","x","y"])

df.id.unique()
print(len(df), len(df.id.unique()))

# Prepare new dataframe, only 1 segment for each youtube ID
df1 = df.groupby('id').head(1)



all_info = []
for row in tqdm(df1.iterrows(), total=len(df1)):
    x = row[1]
    vidinfos = VidInfo(x[0], float(x[1]), float(x[2]), OUT_DIR)
    all_info.append(vidinfos)
    
    # download(vidinfos)


bad_files = open('bad_files_{}.txt'.format(split), 'w')
results = ThreadPool(5).imap_unordered(download, all_info)
cnt = 0
for r in results:
    cnt += 1
    print(cnt, '/', len(all_info), r)
    if 'ERROR' in r:
        bad_files.write(r + '\n')
bad_files.close()
