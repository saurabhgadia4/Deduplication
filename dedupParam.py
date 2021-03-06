SEARCH_COMMAND = "Usage: python dedupSearch.py -s segment_path -n nutch_home_dir -o output_dir"
NUTCH_HOME = ""
NUTCH_BIN = ""
REL_BIN_PATH = "runtime/local/bin"
INV_NUTCH_PATH = "Nutch home path is invalid"
INV_SEG_PATH = "Segment path is invalid"
INV_OUTDIR_PATH = "output directory path is invalid"
GEN_PARSE_DUMP = "/nutch readseg -dump "
PARSE_ARGS = " -nocontent -nofetch -nogenerate -noparsedata -noparse"
GEN_PARSE_ERROR = "Error while generating parse dump for segment "
NUM_REC_THREAD = 1
NUM_SEG_THREAD = 8
DUMP_DIR_PREFIX = 'dump-seg'
REGEXP_PUNC = '\...|\-|\,|\|'
DEFAULT_KGRAM = 3
DEFAULT_THRESH = 0.70
SET_FIELD = 1
URL_FIELD = 0

