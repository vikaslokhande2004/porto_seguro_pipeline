import os
import json
import logging
import pandas as pd
from anthropic import Anthropic


# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/note_extractor.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Anthropic client
# Reads ANTHROPIC_API_KEY from environment
# ---------------------------------------------------------
client = Anthropic()


# ---------------------------------------------------------
# Extraction prompt
# ---------------------------------------------------------
EXTRACTION_PROMPT = """
Extract structured fields from this insurance
adjuster note. Return ONLY valid JSON, no
other text, in this exact shape:

{{
  "incident_category": one of ["collision",
      "weather", "theft", "vandalism",
      "fire", "animal_strike", "road_hazard"],
  "at_fault_party": one of ["insured",
      "other_party", "unknown", "not_applicable"],
  "injury_mentioned": true or false,
  "severity_hint": one of ["minor", "moderate",
      "severe", "total_loss"]
}}

Note: {note_text}
"""


# ---------------------------------------------------------
# Extract structured fields from one adjuster note
# ---------------------------------------------------------
def extract_fields(note_text: str) -> dict:

    logger.info("LLM extraction started")

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=200,
            messages=[
                {
                    "role": "user",
                    "content": EXTRACTION_PROMPT.format(
                        note_text=note_text
                    )
                }
            ]
        )

        raw = response.content[0].text.strip()

        logger.info("LLM extraction completed successfully")

        return json.loads(raw)

    except json.JSONDecodeError:
        logger.error(
            "JSON parse failed. Raw response: %s",
            raw
        )

        return {
            "error": "parse_failed",
            "raw_response": raw
        }

    except Exception as e:
        logger.error(
            "LLM extraction failed: %s",
            str(e)
        )

        return {
            "error": str(e)
        }


# ---------------------------------------------------------
# Process all adjuster notes
# ---------------------------------------------------------
def process_all_notes(filepath: str) -> pd.DataFrame:

    logger.info(
        "Starting note processing: %s",
        filepath
    )

    df = pd.read_csv(filepath)

    results = []

    for _, row in df.iterrows():

        claim_id = row["claim_id"]
        note_text = row["note_text"]

        logger.info(
            "Processing claim_id=%s",
            claim_id
        )

        extracted = extract_fields(note_text)

        extracted["claim_id"] = claim_id

        results.append(extracted)

    result_df = pd.DataFrame(results)

    logger.info(
        "Finished processing %d notes",
        len(result_df)
    )

    return result_df


# ---------------------------------------------------------
# Main execution
# ---------------------------------------------------------
if __name__ == "__main__":

    input_file = "silver/adjuster_notes.csv"
    output_file = "silver/adjuster_notes_structured.csv"

    result_df = process_all_notes(input_file)

    print("\nStructured Adjuster Notes:")
    print("=" * 80)
    print(result_df.to_string(index=False))

    result_df.to_csv(
        output_file,
        index=False
    )

    print("\nOutput written to:")
    print(output_file)