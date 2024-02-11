import pandas as pd
import json
from os import listdir


def listsplit(s: str, sep: str):
    return list(map(lambda el: el.replace(" ", ""), s.split(sep)))


def firstnumber(s: str):
    return int(s.split(" ")[0].replace(",", ""))


def distance(s: str):
    if "km" in s:
        s = s.replace("km", "")
        return float(s)
    elif "m" in s:
        s = s.replace("m", "")
        return float(s)/1000
    return


def remove_stagenumber(s: str):
    return "".join(list(filter(lambda x: not (x.isdigit() or x == '.'), s))).strip()


def scraping(link: str, tableindex: int, write: bool):
    dfs = pd.read_html(link)
    df = dfs[tableindex]
    # save into temp fail to avoid loading time
    if write:
        df.to_json("buffer.json")
    return df


def cleanup(file: str, option: int):
    df = pd.read_json(file)
    newdf = pd.DataFrame()
    if option == 0:
        newdf.set_index("Nme")
        newdf["Route"] = df["Route"].apply(lambda s: listsplit(s, "•"))
        newdf["Length"] = df["Length"].apply(firstnumber)
        # df.rename(columns={x: ("Name" if x == "#" else x) for x in df.columns},
        #         inplace=True)
    elif option == 1:
        # split row into two and clean up
        newdf[["Start", "Finish"]] = df["Stage"].str.split(' - ', n=1, expand=True)
        newdf["Start"] = newdf["Start"].apply(remove_stagenumber)
        newdf["Finish"] = newdf["Finish"].apply(lambda s: s.strip())
        # adding other data
        newdf["Distance"] = df["Distance"].apply(distance)
        newdf["Total climb"] = df["Total climb"].apply(distance)
        newdf["Total descent"] = df["Total descent"].apply(
            lambda s: distance(s.split(" ")[0]))
    return newdf


def check_trail_data(trailfn: str):
    # check all tables
    lastfinish = ""
    for fn in sorted(listdir(f"data/{trailfn}")):
        print(fn)
        df = pd.read_csv(f"data/{trailfn}/{fn}", index_col=0)
        if df["Start"][0] != lastfinish and lastfinish != "":
            print("parts check", df["Start"][0], lastfinish)
        for i in range(df.shape[0]-1):
            if df["Finish"][i] != df["Start"][i+1]:
                print("check", i, df["Finish"][i])
        lastfinish = df["Finish"][df.shape[0]-1]


kmtomile = 0.62137
e1_norwayfinlandsweden = ['finnmark', 'nordkalottleden-north', 'nordkalottleden-central',
                          'nordkalottleden-south', 'southern-nordlandsruta-borgefjell',
                          'nord-trondelag', 'sor-trondelag-hedmark', 'dalarnas-laen',
                          'oerebro-laen', 'vaestra-goetalands-laen-joenkoepings-laen',
                          'hallands-laen']
e1_denmark = ['central-jutland', 'south-denmark']
e1_germany = ['schleswig-holstein', 'hamburg', 'nordheide', 'suedheide-und-hanoverian-bergland',
              'lippe-uplands-teutoburg-forest-egge-hills', 'sauerland', 'westerwald',
              'taunus', 'odenwald', 'black-forest']
e1_switzerland = ['northeastern-switzerland',
                  'central-switzerland', 'southern-switzerland']
e1_italy = ['lombardy', 'piedmont', 'liguria', 'emilia-romagna-tuscany', 'umbria',
            'lazio-north', 'abruzzo', 'lazio-south-molise', 'campania', 'sicily']

if __name__ == "__main__":
    section = "italy"
    # for i, stage in enumerate(e1_italy):
    #     link = f"https://e1.hiking-europe.eu/en/e1/stages/{section}/{stage}"
    #     scrape = True
    #     print(link)
    #     # reading in page
    #     df = scraping(link, 0, scrape)
    #     # print(df["Stage"].apply(remove_stagenumber))
    #     df = cleanup("buffer.json", 1)
    #     df.to_csv(f"data/e1/{section}/{i+1:02}_{stage}.csv")

    # check_trail_data(f"e1/{section}")
    e1dict = {"sections": ["norway-finland-sweden", "denmark", "germany", "switzerland", "italy"],
              "stages": {"norway-finland-sweden": e1_norwayfinlandsweden,
                         "denmark": e1_denmark,
                         "germany": e1_germany,
                         "switzerland": e1_switzerland,
                         "italy": e1_italy}}
    
    with open("data/e1/e1_overview.json", "w") as fp:
        json.dump(e1dict , fp) 