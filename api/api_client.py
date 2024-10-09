from datetime import datetime
from dateutil.relativedelta import relativedelta
from tardis_dev import datasets, get_exchange_details
from tqdm import tqdm
import logging

# comment out to disable debug logs
logging.basicConfig(level=logging.DEBUG)


def increment_dt(dt_str, option):
    date = datetime.strptime(dt_str, "%Y-%m-%d")
    new_date = date + \
        relativedelta(months=1) if option == "months" else date + \
        relativedelta(days=1)
    return new_date.strftime("%Y-%m-%d")


def download_options(exchange, data_types, from_dt, to_dt, symbols, api_key=None):
    try:
        datasets.download(
            exchange=exchange,
            data_types=data_types,
            from_date=from_dt,
            to_date=to_dt,
            symbols=symbols,
            api_key=api_key
        )
    except Exception as e:
        logging.error(f"Error downloading options: {e}")


def main():
    initial_dt = "2019-12-01"
    final_dt = "2024-10-01"

    available_hist = []

    while initial_dt <= final_dt:
        available_hist.append(initial_dt)
        initial_dt = increment_dt(initial_dt, "months")

    for from_dt in tqdm(available_hist):
        to_dt = increment_dt(from_dt, "days")
        download_options("deribit", ["options_chain"],
                         from_dt, to_dt, ["OPTIONS"], None)


if __name__ == "__main__":
    main()
