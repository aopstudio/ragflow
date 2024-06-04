#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import re
from copy import deepcopy
from io import BytesIO
from timeit import default_timer as timer
from nltk import word_tokenize
from openpyxl import load_workbook
from rag.nlp import is_english, random_choices, find_codec, bullets_category, qbullets_category, not_question_bullet
from rag.nlp import rag_tokenizer, tokenize_table
from rag.settings import cron_logger
from deepdoc.parser import PdfParser, ExcelParser

class Excel(ExcelParser):
    def __call__(self, fnm, binary=None, callback=None):
        if not binary:
            wb = load_workbook(fnm)
        else:
            wb = load_workbook(BytesIO(binary))
        total = 0
        for sheetname in wb.sheetnames:
            total += len(list(wb[sheetname].rows))

        res, fails = [], []
        for sheetname in wb.sheetnames:
            ws = wb[sheetname]
            rows = list(ws.rows)
            for i, r in enumerate(rows):
                q, a = "", ""
                for cell in r:
                    if not cell.value:
                        continue
                    if not q:
                        q = str(cell.value)
                    elif not a:
                        a = str(cell.value)
                    else:
                        break
                if q and a:
                    res.append((q, a))
                else:
                    fails.append(str(i + 1))
                if len(res) % 999 == 0:
                    callback(len(res) *
                             0.6 /
                             total, ("Extract Q&A: {}".format(len(res)) +
                                     (f"{len(fails)} failure, line: %s..." %
                                      (",".join(fails[:3])) if fails else "")))

        callback(0.6, ("Extract Q&A: {}. ".format(len(res)) + (
            f"{len(fails)} failure, line: %s..." % (",".join(fails[:3])) if fails else "")))
        self.is_english = is_english(
            [rmPrefix(q) for q, _ in random_choices(res, k=30) if len(q) > 1])
        return res

class Pdf(PdfParser):
    def __call__(self, filename, binary=None, from_page=0,
                 to_page=100000, zoomin=3, callback=None):
        start = timer()
        callback(msg="OCR is running...")
        self.__images__(
            filename if not binary else binary,
            zoomin,
            from_page,
            to_page,
            callback
        )
        callback(msg="OCR finished")
        cron_logger.info("OCR({}~{}): {}".format(from_page, to_page, timer() - start))

        start = timer()
        self._layouts_rec(zoomin)
        callback(0.63, "Layout analysis finished.")
        self._table_transformer_job(zoomin)
        callback(0.65, "Table analysis finished.")
        self._text_merge()
        callback(0.67, "Text merging finished")
        tbls = self._extract_table_figure(True, zoomin, True, True)
        #self._naive_vertical_merge()
        self._concat_downward()
        #self._filter_forpages()

        cron_logger.info("layouts: {}".format(timer() - start))
        sections = [b["text"] for b in self.boxes]
        q_bull, reg = qbullets_category(sections)
        if q_bull == -1:
            raise ValueError("Unable to recognize Q&A structure.")
        q_reg = r'(\w|\W)*?(?:？|\?|\n|$)+'
        full_reg = reg + q_reg
        qa_list = []
        last_q, last_a = '', ''
        for section in sections:
            has_bull = re.search(full_reg, section)
            if not has_bull or not_question_bullet(section):  # No question bullet
                if not last_q:
                    continue
                else:
                    last_a = f'{last_a}{section}'
            else:
                re_iter = re.finditer(full_reg, section)
                last_end = 0
                for re_obj in re_iter:
                    start, end = re_obj.span()
                    if start == 0 and last_q:   # last question is end but not submitted
                        qa_list.append((last_q, last_a)) # submit last question
                        last_q, last_a = '', '' # empty last question
                    former = section[last_end:start]
                    last_end = end
                    if last_q:  # last question not end
                        last_a = f'{last_a}{former}'
                        qa_list.append((last_q, last_a)) # submit last question
                        last_q, last_a = '', '' # empty last question
                    else:
                        last_q = re_obj.group().lstrip(re.match(reg,re_obj.group()).group())    # remove bullet index
                if last_end != len(section):
                    remainer = section[last_end:]
                    last_a = f'{last_a}{remainer}'
        if last_q:
            qa_list.append((last_q, last_a))
        return qa_list
    
def rmPrefix(txt):
    return re.sub(
        r"^(问题|答案|回答|user|assistant|Q|A|Question|Answer|问|答)[\t:： ]+", "", txt.strip(), flags=re.IGNORECASE)


def beAdoc(d, q, a, eng):
    qprefix = "Question: " if eng else "问题："
    aprefix = "Answer: " if eng else "回答："
    d["content_with_weight"] = "\t".join(
        [qprefix + rmPrefix(q), aprefix + rmPrefix(a)])
    d["content_ltks"] = rag_tokenizer.tokenize(q)
    d["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(d["content_ltks"])
    return d


def chunk(filename, binary=None, lang="Chinese", callback=None, **kwargs):
    """
        Excel and csv(txt) format files are supported.
        If the file is in excel format, there should be 2 column question and answer without header.
        And question column is ahead of answer column.
        And it's O.K if it has multiple sheets as long as the columns are rightly composed.

        If it's in csv format, it should be UTF-8 encoded. Use TAB as delimiter to separate question and answer.

        All the deformed lines will be ignored.
        Every pair of Q&A will be treated as a chunk.
    """
    eng = lang.lower() == "english"
    res = []
    doc = {
        "docnm_kwd": filename,
        "title_tks": rag_tokenizer.tokenize(re.sub(r"\.[a-zA-Z]+$", "", filename))
    }
    if re.search(r"\.xlsx?$", filename, re.IGNORECASE):
        callback(0.1, "Start to parse.")
        excel_parser = Excel()
        for q, a in excel_parser(filename, binary, callback):
            res.append(beAdoc(deepcopy(doc), q, a, eng))
        return res
    elif re.search(r"\.(txt|csv)$", filename, re.IGNORECASE):
        callback(0.1, "Start to parse.")
        txt = ""
        if binary:
            encoding = find_codec(binary)
            txt = binary.decode(encoding, errors="ignore")
        else:
            with open(filename, "r") as f:
                while True:
                    l = f.readline()
                    if not l:
                        break
                    txt += l
        lines = txt.split("\n")
        comma, tab = 0, 0
        for l in lines:
            if len(l.split(",")) == 2: comma += 1
            if len(l.split("\t")) == 2: tab += 1
        delimiter = "\t" if tab >= comma else ","

        fails = []
        question, answer = "", ""
        i = 0
        while i < len(lines):
            arr = lines[i].split(delimiter)
            if len(arr) != 2:
                if question: answer += "\n" + lines[i]
                else:
                    fails.append(str(i+1))
            elif len(arr) == 2:
                if question and answer: res.append(beAdoc(deepcopy(doc), question, answer, eng))
                question, answer = arr
            i += 1
            if len(res) % 999 == 0:
                callback(len(res) * 0.6 / len(lines), ("Extract Q&A: {}".format(len(res)) + (
                    f"{len(fails)} failure, line: %s..." % (",".join(fails[:3])) if fails else "")))

        if question: res.append(beAdoc(deepcopy(doc), question, answer, eng))

        callback(0.6, ("Extract Q&A: {}".format(len(res)) + (
            f"{len(fails)} failure, line: %s..." % (",".join(fails[:3])) if fails else "")))

        return res
    elif re.search(r"\.pdf$", filename, re.IGNORECASE):
        pdf_parser = Pdf()
        for q, a in pdf_parser(filename if not binary else binary,
                                    from_page=0, to_page=10000, callback=callback):
            res.append(beAdoc(deepcopy(doc), q, a, eng))
        return res


    raise NotImplementedError(
        "Excel and csv(txt) format files are supported.")


if __name__ == "__main__":
    import sys

    def dummy(prog=None, msg=""):
        pass
    chunk(sys.argv[1], from_page=0, to_page=10, callback=dummy)
