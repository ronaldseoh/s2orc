import gzip
import argparse

import jsonlines


if __name__ == '__main__':

    ap = argparse.ArgumentParser()
    ap.add_argument('--data-dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
