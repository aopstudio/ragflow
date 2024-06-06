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
from rag.nlp import is_english, random_choices, find_codec, bullets_category, qbullets_category, not_question_bullet, add_positions, has_qbullet
from rag.nlp import rag_tokenizer, tokenize_table
from rag.settings import cron_logger
from deepdoc.parser import PdfParser, ExcelParser
from icecream import ic
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
        # self.page_images, self.boxes = self.page_images[-2:-1], self.boxes[-2:-1]
        start = timer()
        self._layouts_rec(zoomin)
        callback(0.63, "Layout analysis finished.")
        self._table_transformer_job(zoomin)
        callback(0.65, "Table analysis finished.")
        self._text_merge()
        callback(0.67, "Text merging finished")
        tbls = self._extract_table_figure(True, zoomin, True, True) # 这里会有table和image，table加到文本，image加到image
        #self._naive_vertical_merge()
        # self._concat_downward()

        #self._filter_forpages()

        cron_logger.info("layouts: {}".format(timer() - start))
        sections = [b["text"] for b in self.boxes]
        # sections = ['66、干细胞抗衰适合哪些人？答：高压力、工作紧张和亚健康的人群；预防衰老，要求维持机体的年轻靓丽，保持面部美容年轻化的人群；内脏器官功能出现退化的人群。例如心、肝、肺、肾、肠胃等器官出现功能衰退和下降的人群；内分泌失调的人群。例如女性出现月经失调、内分泌紊乱、卵巢早衰、更年期提前，睡眠和情绪都不是很好的情况；免疫系统出现退化的人群。例如免疫力比较弱，经常爱感冒的人；机体出现未老先衰的情况的人群。例如机体衰老，缺乏活力，比较容易产生疲惫，组织器官等功能出现老化等；骨骼等运动系统退变的人群。例如骨关节退变、关节炎、骨质疏松、骨关节增生疼痛、肌肉、韧带、肌腱功能退化，运动及活动能力下降等；心血管系统发生退变的人群。例如冠状动脉硬化、动脉硬化、老化、狭窄级及压增高等。67、干细胞抗衰不适合哪些人（禁忌症）？答：高度过敏体质或有严重过敏史者；休克、全身衰竭，以及不能配合检查者；晚期恶性肿瘤；有全身感染性疾患者，需控制感染后再行干细胞抗衰方案；有凝血功能障碍性疾病，如血友病等；有严重的精神障碍者。', '68、干细胞对于一些溃疡有疗效吗？是通过什么方式进行治疗？', '溃疡是皮肤或黏膜表面组织的限局性缺损、溃烂，可通过局部种植间充质干细胞进行治疗。间充质干细胞具有多向分化能力，且其强大的免疫调节功能，广谱的细胞因子分泌以及造血支持的特点，种植的干细胞可以抑制创口的炎症反应，促进新生血管生成，提供营养支持，促进组织的修复和创口的愈合。69、对于大面积烧伤的病人，干细胞能用于烧伤部位的修复吗？干细胞治疗可以促进烧伤伤疤愈合。SkinGun皮肤修复枪就是干细胞治疗皮肤烧伤的疗法。间充质干细胞可以分化为表皮细胞，成纤维细胞，肌肉细胞；通过免疫调节控制炎症，通过旁分泌作用影响成纤维细胞的增殖、胶原合成及迁移能力，促进肉芽组织形成和血管新生，达到皮肤的修复和再生。', '70、干细胞能用于改善过敏体质吗？过敏体质从免疫学角度看，是由于免疫异常造成。间充质干细胞具有强大的免疫条件作用，对于一般过敏体质，干细胞可以改善。71、听说30 岁以下的人不适合打干细胞，为什么？理论上，干细胞可适用于任何年龄阶段的人群。干细胞安全，在治疗疾病和抗衰老和美容等方面具有很好的应用价值。个人可根据自身健康需求使用干细胞。72、对于先天性缺陷疾病，干细胞能不能缓解症状？先天性疾病是胎儿在子宫内的生长发育过程中，受到外界或内在不良因素作用，致使胎儿发育不正常，不是遗传性疾病，例如部分脑发育不良、视神经发育不良等。干细胞可以分化成功能细胞，修复或替代受损的组织或器官，同时分泌大量的细胞因子，起到营养支持等作用，达到良好的治疗效果。73、干细胞用于美容时多久能见到效果根据使用方式以及个体差异，效果显示有出现。74、干细胞治疗对任何疾病都能达到缓解的效果吗干细胞治疗疾病，由于病种病因病程以及个人差异问题，治疗效果因人而异。75、干细胞输入人体后，会不会体内失控，导致癌变？不会。干细胞存在人体，本身是安全的。输入的是成体干细胞，成体干细胞虽然具有多向分化潜能，但是其分化能力和分化方向可控，近50年的应用验证，是十分安全的。76、客户在治疗前需要做些什么？咨询答疑，陈述需求；根据体检要求去医院或指定医疗机构进行深度体检；根据治疗方案交付费用，签署知情同意书，保养身体，确保回输时身命体征正常。', 'RSTC']
        line_tags = [self._line_tag(b, zoomin) for b in self.boxes]
        q_bull, reg = qbullets_category(sections)
        if q_bull == -1:
            raise ValueError("Unable to recognize Q&A structure.")
        qai_list = []
        last_q, last_a, last_tag = '', '', ''
        end_reg = r'[,.;，。；]$'
        last_index = -1
        last_section = ''
        for box in self.boxes:
            section, line_tag = box['text'], self._line_tag(box, zoomin)
            has_bull, index = has_qbullet(reg, last_section)
            last_section = section
            if has_bull:
                if index > last_index:
                    last_index = index
                else:
                    has_bull = False
            if not has_bull:  # No question bullet
                if not last_q:
                    continue
                else:
                    last_a = f'{last_a}{section}'
                    last_tag = f'{last_tag}{line_tag}'
            else:
                if last_q:
                    qai_list.append((last_q, last_a, *self.crop(last_tag, need_position=True)))
                    last_q, last_a, last_tag = '', '', ''
                last_q = has_bull.group()
                _, end = has_bull.span()
                last_a = section[end:]
                last_tag = line_tag
        if last_q:
            qai_list.append((last_q, last_a, *self.crop(last_tag, need_position=True)))
        return qai_list
    
def rmPrefix(txt):
    return re.sub(
        r"^(问题|答案|回答|user|assistant|Q|A|Question|Answer|问|答)[\t:： ]+", "", txt.strip(), flags=re.IGNORECASE)


def beAdocPdf(d, q, a, eng, image, poss):
    qprefix = "Question: " if eng else "问题："
    aprefix = "Answer: " if eng else "回答："
    d["content_with_weight"] = "\t".join(
        [qprefix + rmPrefix(q), aprefix + rmPrefix(a)])
    d["content_ltks"] = rag_tokenizer.tokenize(q)
    d["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(d["content_ltks"])
    d["image"] = image
    add_positions(d, poss)
    return d

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
        count = 0
        for q, a, image, poss  in pdf_parser(filename if not binary else binary,
                                    from_page=0, to_page=10000, callback=callback):
            count += 1
            res.append(beAdocPdf(deepcopy(doc), q, a, eng, image, poss))
        return res


    raise NotImplementedError(
        "Excel and csv(txt) format files are supported.")


if __name__ == "__main__":
    import sys

    def dummy(prog=None, msg=""):
        pass
    ic(chunk(sys.argv[1], from_page=0, to_page=10, callback=dummy)[:10])
