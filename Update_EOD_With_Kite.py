import pandas as pd
from pathlib import Path

# ------------------------------
# Configuration
# ------------------------------
target_date = "2025-11-14"  # 👈 specify only date
date_folder = pd.Timestamp(target_date).strftime("%Y-%m-%d")

# Token-to-symbol mapping
token_map = {
    3329:"ABB",
5633:"ACC",
6401:"ADAENT",
40193:"APOHOS",
54273:"ASHOKLEY",
60417:"ASIPAI",
70401:"AURPHA",
78081:"BAJHOL",
79873:"TUBIN",
81153:"BAJFI",
98049:"BHAELE",
108033:"BHAFOR",
112129:"BHEL",
119553:"HDFSTA",
130305:"MAZDOC",
134657:"BHAPET",
140033:"BRIIND",
173057:"EXIIND",
175361:"CHOINV",
177665:"CIPLA",
189185:"CORENG",
194561:"CROGRE",
197633:"DABIND",
225537:"DRREDD",
232961:"EICMOT",
261889:"FEDBAN",
302337:"GODPHI",
315393:"GRASIM",
325121:"AMBCE",
341249:"HDFBAN",
345089:"HERHON",
348929:"HINDAL",
356865:"HINLEV",
359937:"HINPET",
364545:"HINZIN",
387073:"INDHOT",
408065:"INFTEC",
415745:"INDOIL",
424961:"ITC",
486657:"CUMIND",
492033:"KOTMAH",
502785:"TRENT",
511233:"LICHF",
519425:"INDR",
519937:"MAHMAH",
548353:"MAXFIN",
548865:"BHADYN",
558337:"BOSLIM",
582913:"MRFTYR",
589569:"HINAER",
633601:"ONGC",
681985:"PIDIND",
738561:"RELIND",
756481:"KALJEW",
758529:"SAIL",
779521:"STABAN",
784129:"VEDLIM",
794369:"SHRCEM",
806401:"SIEMEN",
824321:"MACDEV",
837889:"SRF",
857857:"SUNPHA",
860929:"SUPIND",
873217:"TATELX",
877057:"TATPOW",
878593:"TATGLO",
884737:"TATMOT",
895745:"TATSTE",
897537:"TITIND",
900609:"TORPHA",
912129:"ADAGRE",
951809:"VOLTAS",
952577:"TATCOM",
969473:"WIPRO",
1041153:"MARLIM",
1076225:"MOTSUM",
1086465:"HDFAMC",
1102337:"SHRTRA",
1152769:"MPHLIM",
1195009:"BANBAR",
1199105:"SONBLW",
1207553:"GAIL",
1214721:"BANIND",
1215745:"CONCOR",
1270529:"ICIBAN",
1304833:"ZOMLIM",
1346049:"INDBA",
1510401:"AXIBAN",
1552897:"ADAGAS",
1629185:"NATALU",
1675521:"FSNECO",
1703937:"PBFINT",
1716481:"ONE97",
1723649:"JINSP",
1850625:"HCLTEC",
1895937:"GLEPHA",
2029825:"CADHEA",
2127617:"BLUIN",
2170625:"TVSMOT",
2426881:"LIC",
2445313:"RAIVIK",
2455041:"POLI",
2478849:"KPITE",
2513665:"HAVIND",
2585345:"GODCON",
2615553:"ADATRA",
2672641:"LUPIN",
2674433:"UNISPI",
2714625:"BHAAIR",
2730497:"PUNBAN",
2748929:"ORAFIN",
2752769:"UNIBAN",
2763265:"CANBAN",
2800641:"DIVLAB",
2815745:"MARUTI",
2863105:"IDFBAN",
2865921:"INTAVI",
2883073:"INDGAS",
2889473:"UNIP",
2911489:"BIOCON",
2939649:"LARTOU",
2952193:"ULTCEM",
2953217:"TCS",
2955009:"NIITEC",
2977281:"NTPC",
2995969:"ALKLAB",
3001089:"JSWSTE",
3050241:"YESBAN",
3076609:"SUZENE",
3343617:"360ONE",
3400961:"MAHFIN",
3407361:"KEIIND",
3412993:"SOLIN",
3463169:"GMRINF",
3465729:"TECMAH",
3484417:"INDRAI",
3520257:"INFEDG",
3529217:"TORPOW",
3660545:"PFC",
3663105:"INDIBA",
3677697:"IDECEL",
3689729:"PAGIND",
3691009:"ASTPOL",
3725313:"PHOMIL",
3735553:"FORHEA",
3771393:"DLFLIM",
3826433:"MOTOSW",
3834113:"POWGRI",
3861249:"ADAPOR",
3876097:"COLPAL",
3920129:"IRBINF",
3924993:"NATMIN",
3930881:"RURELE",
3937281:"MAPHA",
4267265:"BAAUTO",
4268801:"BAFINS",
4359425:"RUCSOY",
4451329:"ADAPOW",
4454401:"NHPC",
4464129:"OILIND",
4561409:"LTINFO",
4574465:"JSWENE",
4576001:"GODPRO",
4598529:"NESIND",
4600577:"SBICAR",
4632577:"JUBFOO",
4644609:"JIOFIN",
4701441:"PERSYS",
4724993:"ABBPOW",
4843777:"VARBEV",
5013761:"BSE",
5097729:"AVESUP",
5181953:"OBEREA",
5186817:"INDREN",
5195009:"TATTEC",
5197313:"PREEST",
5215745:"COALIN",
5331201:"HUDCO",
5436929:"AUSMA",
5506049:"COCSHI",
5533185:"ABCAPITAL",
5552641:"DIXTEC",
5573121:"ICILOM",
5582849:"SBILIFE",
5728513:"MAXHEA",
6013185:"BHAHEX",
6054401:"MUTFIN",
6191105:"PIIND",
6386689:"LTFINA",
6412545:"PREENR",
6469121:"BAJHOU",
6599681:"APLAPO",
6616065:"HYUMOT",
6632193:"WAAENE",
6928897:"SWILIM",
6957057:"NTPGRE",
7160065:"VISMEG",
7458561:"BHAINF",
7488257:"ITCHOT",
193758977:"SIEENE"
}

# Base directories
base_dir = Path(r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup")
daily_dir = base_dir / "daily" / date_folder
hourly_dir = base_dir / "hourly" / date_folder

# Historical data directory
fixed_base_dir = Path("historicaldata")

# ------------------------------
# Processing loop
# ------------------------------
for token, symbol in token_map.items():
    try:
        print(f"\n🔄 Processing {symbol} ({token}) for date {target_date} ...")

        # Define paths
        fixed_path = fixed_base_dir / f"{symbol}_fixed.parquet"
        daily_path = daily_dir / f"{token}.parquet"
        hourly_path = hourly_dir / f"{token}.parquet"

        print(f"Fixed path: {fixed_path}")
        print(f"Daily path: {daily_path}")
        print(f"Hourly path: {hourly_path}")

        # --- Read data ---
        fixed_df = pd.read_parquet(fixed_path)

        # ✅ Ensure datetime column exists
        if fixed_df.index.name == "datetime":
            fixed_df = fixed_df.reset_index()
        elif "datetime" not in fixed_df.columns:
            raise KeyError(f"'datetime' column not found in {symbol}_fixed.parquet.")

        daily_df = pd.read_parquet(daily_path)
        hourly_df = pd.read_parquet(hourly_path)

        # --- Extract OHLC from daily rollup ---
        ohlc_row = daily_df.iloc[-1][["ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close"]]

        # --- Get latest timestamp from hourly data ---
        hourly_df["exchange_timestamp"] = pd.to_datetime(hourly_df["exchange_timestamp"])
        target_timestamp = hourly_df["exchange_timestamp"].max()

        # --- Extract volume for latest timestamp ---
        volume_value = hourly_df.loc[
            hourly_df["exchange_timestamp"] == target_timestamp, "volume_traded"
        ].iloc[0]

        # --- Create new row ---
        new_row = {
            "datetime": target_timestamp,
            "Open": ohlc_row["ohlc_open"],
            "High": ohlc_row["ohlc_high"],
            "Low": ohlc_row["ohlc_low"],
            "Close": ohlc_row["ohlc_close"],
            "Volume": volume_value,
        }

        # --- Update or append ---
        if (fixed_df["datetime"] == target_timestamp).any():
            fixed_df.loc[
                fixed_df["datetime"] == target_timestamp,
                ["Open", "High", "Low", "Close", "Volume"],
            ] = [
                new_row["Open"],
                new_row["High"],
                new_row["Low"],
                new_row["Close"],
                new_row["Volume"],
            ]
            print(f"📝 Updated existing record for {symbol} at {target_timestamp}.")
        else:
            fixed_df = pd.concat([fixed_df, pd.DataFrame([new_row])], ignore_index=True)
            print(f"➕ Added new record for {symbol} at {target_timestamp}.")

        # ✅ Revert datetime to index before saving (preserve original structure)
        if "datetime" not in fixed_df.columns and fixed_df.index.name == "datetime":
            pass  # already correct
        elif "datetime" not in fixed_df.columns:
            raise KeyError(f"'datetime' column missing for {symbol} before saving.")

        fixed_df = fixed_df.set_index("datetime")
        fixed_df.to_parquet(fixed_path)  # ✅ no index=False → keeps datetime index

        print(f"✅ Saved successfully → {fixed_path}")

    except FileNotFoundError as e:
        print(f"❌ Missing file for {symbol}: {e}")
    except Exception as e:
        print(f"⚠️ Error processing {symbol}: {e}")

print("\n🏁 All instruments processed successfully.")
