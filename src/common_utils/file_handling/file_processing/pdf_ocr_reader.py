from pathlib import Path

import PyPDF2


def pdf_to_text(pdf_path: Path):
    with open(pdf_path, "rb") as file:
        pdf_reader = PyPDF2.PdfReader(file)
        num_pages = len(pdf_reader.pages)

        # with open(output_path, "w", encoding="utf-8") as out_file:
        pdf_str = ""
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]

            text = page.extract_text()

            pdf_str += f"--- Page {page_num + 1} ---\n"
            pdf_str += text
            pdf_str += "\n"
        return pdf_str


# if __name__ == "__main__":
#     input_pdf = "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/extractors/_checkpoint/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB/last_start_46000/2024-08-07T08:19:44Z_A_Logical_Fallacy_Informed_Framework_for/paper_2024-08-07T08:19:44Z_A_Logical_Fallacy_Informed_Framework_for.pdf"
#     pdf_to_text(input_pdf, "./pdf.txt")
