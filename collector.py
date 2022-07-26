#!/usr/bin/python3
import datetime, sys, requests, json, subprocess
import mysql.connector

# Date Info (Created here so all entries are the same)
now = datetime.datetime.utcnow()
now_timestamp = now.strftime('%Y-%m-%d %H:%M:00')

# Connect To the database & handle SQL connection errors
cnx=None; cursor=None
def connectToDB():
    global cnx; global cursor
    try:
        print("DB: Connecting")
        cnx = mysql.connector.connect(
            user='grafana', password='22spss4u!',
            host='127.0.0.1', database='collector')
        print("DB: Connected")
        return cnx
    except mysql.connector.Error as err:
        print(err)

# Log data to the collector DB Table
logCollectorEntrySQL = """
    INSERT INTO collector (
        collector_datetime, collector_source_name, collector_owner,
        collector_machine_id, collector_machine_name, collector_spot_revenue_hr,
        collector_spot_revenue_day, collector_spot_revenue_month,
        collector_gpus_spot_rented, collector_gpus_ondemand_rented
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
def logCollectorEntry(source_name, owner, machine_id, machine_name, rev_hr, rev_day, rev_month, gpu_spot=0, gpu_ondemand=0):
    global cnx; global cursor
    if cnx is None: cnx = connectToDB()
    if cursor is None: cursor = cnx.cursor()
    rev_hr = round(rev_hr,2)
    rev_day = round(rev_day,2)
    rev_month = round(rev_month,2)
    print(f"DB: Logging entry for [{owner}][{source_name}][{machine_name}][${rev_hr}/hr][${rev_day}/day][${rev_month}/month][spot: {gpu_spot}][demand: {gpu_ondemand}]")
    val = (
        now_timestamp, source_name, owner, machine_id, machine_name,
        round(rev_hr,2), round(rev_day,2), round(rev_month,2),
        gpu_spot, gpu_ondemand
    )
    cursor.execute(logCollectorEntrySQL, val);
    cnx.commit()

def ethHashToCoin(hashrate):
    url = f"https://whattomine.com/coins.json?eth=true&factor%5Beth_hr%5D={round(hashrate,2)}&factor%5Beth_p%5D=0.0&e4g=true&factor%5Be4g_hr%5D=0.0&factor%5Be4g_p%5D=0.0&zh=true&factor%5Bzh_hr%5D=0.0&factor%5Bzh_p%5D=0.0&cnh=true&factor%5Bcnh_hr%5D=0.0&factor%5Bcnh_p%5D=0.0&cng=true&factor%5Bcng_hr%5D=0.0&factor%5Bcng_p%5D=0.0&cnf=true&factor%5Bcnf_hr%5D=0.0&factor%5Bcnf_p%5D=0.0&cx=true&factor%5Bcx_hr%5D=0.0&factor%5Bcx_p%5D=0.0&eqa=true&factor%5Beqa_hr%5D=0.0&factor%5Beqa_p%5D=0.0&cc=true&factor%5Bcc_hr%5D=0.0&factor%5Bcc_p%5D=0.0&cr29=true&factor%5Bcr29_hr%5D=0.0&factor%5Bcr29_p%5D=0.0&ct31=true&factor%5Bct31_hr%5D=0.0&factor%5Bct31_p%5D=0.0&ct32=true&factor%5Bct32_hr%5D=0.0&factor%5Bct32_p%5D=0.0&eqb=true&factor%5Beqb_hr%5D=0.0&factor%5Beqb_p%5D=0.0&rmx=true&factor%5Brmx_hr%5D=0.0&factor%5Brmx_p%5D=0.0&ns=true&factor%5Bns_hr%5D=0.0&factor%5Bns_p%5D=0.0&al=true&factor%5Bal_hr%5D=0.0&factor%5Bal_p%5D=0.0&ops=true&factor%5Bops_hr%5D=0.0&factor%5Bops_p%5D=0.0&eqz=true&factor%5Beqz_hr%5D=0.0&factor%5Beqz_p%5D=0.0&zlh=true&factor%5Bzlh_hr%5D=0.0&factor%5Bzlh_p%5D=0.0&kpw=true&factor%5Bkpw_hr%5D=0.0&factor%5Bkpw_p%5D=0.0&ppw=true&factor%5Bppw_hr%5D=0.0&factor%5Bppw_p%5D=0.0&x25x=true&factor%5Bx25x_hr%5D=0.0&factor%5Bx25x_p%5D=0.0&fpw=true&factor%5Bfpw_hr%5D=0.0&factor%5Bfpw_p%5D=0.0&vh=true&factor%5Bvh_hr%5D=0.0&factor%5Bvh_p%5D=0.0&factor%5Bcost%5D=0.0&factor%5Bcost_currency%5D=USD&sort=Profitability24&volume=0&revenue=24h&factor%5Bexchanges%5D%5B%5D=&factor%5Bexchanges%5D%5B%5D=binance&dataset="
    requestData = requests.get(url)
    stats = json.loads(requestData.text)
    minedEthPerDay = stats["coins"]["Ethereum"]["estimated_rewards24"]
    return minedEthPerDay

def kdaHashToUSD(hashrate):
    url = f"https://whattomine.com/coins/334.json?hr={hashrate}&p=0.0&fee=0.0&cost=0.0&cost_currency=USD&hcost=0.0&span_br=&span_d=24"
    requestData = requests.get(url)
    stats = json.loads(requestData.text)
    usdPerDay = float(stats["revenue"].replace("$", ""))
    return usdPerDay

def ethCoinToUsd(coin):
    url = f"https://min-api.cryptocompare.com/data/blockchain/mining/calculator?fsyms=ETH&tsyms=USD"
    requestData = requests.get(url)
    stats = json.loads(requestData.text)
    ethPrice = stats["Data"]["ETH"]["Price"]["USD"]
    return float(coin)*float(ethPrice)

# converts hashrate into hourly/daily/monthly USD
def coinHashrateToEarnings(coinName, hashrate):
    print(f"COIN: Converting {coinName} hashrate of {hashrate} to USD")
    if coinName == "ethereum":
        coinPerDay = ethHashToCoin(hashrate/1000000)
        daily = ethCoinToUsd(coinPerDay)
        hourly = daily/24
        monthly = daily*30
        return (hourly, daily, monthly)
    elif coinName == "kadena":
        daily = kdaHashToUSD(hashrate)
        hourly = daily/24
        monthly = daily*30
        return (hourly, daily, monthly)
    else:
        # Get the data from coin calculators
        url = f"https://www.coincalculators.io/api?name={coinName}&hashrate={hashrate}"
        requestData = requests.get(url)
        stats = json.loads(requestData.text)
        # parse the data from coin calculators
        hourly = stats["profitInHourUSD"]
        daily = hourly*24
        monthly = daily*30
        return (hourly, daily, monthly)

def getPoolflareIncome(wallet_address):
    print(f"POOLFLARE: Getting stats for wallet {wallet_address}")
    # Get the account data from 2miners
    url = f"https://poolflare.net/api/v1/coin/kda/account/{wallet_address}/stats"
    requestData = requests.get(url)
    stats = json.loads(requestData.text)
    hashrate = stats["data"]["hr"]/1000000000
    (hourly, daily, monthly) = coinHashrateToEarnings("kadena", hashrate)
    return (hourly, daily, monthly)

def logPoolflareIncome(wallet_address, owner):
    (hourly, daily, monthly) = getPoolflareIncome(wallet_address)
    hourly = round(hourly,2); daily = round(daily,2); monthly = round(monthly,2)
    logCollectorEntry("poolflare", owner, 0, "all_miners", hourly, daily, monthly)

# get hourly/daily/monthly income from 2miners wallet
def get2MinersIncome(wallet_address):
    print(f"2MINERS: Getting stats for wallet {wallet_address}")
    # Get the account data from 2miners
    url = f"https://eth.2miners.com/api/accounts/{wallet_address}"
    requestData = requests.get(url)
    stats = json.loads(requestData.text)
    # Parse the account data
    hashrate = (stats["currentHashrate"])
    (hourly, daily, monthly) = coinHashrateToEarnings("ethereum", hashrate)
    return (hourly, daily, monthly)

def log2MinersIncome(wallet_address, owner):
    (hourly, daily, monthly) = get2MinersIncome(wallet_address)
    hourly = round(hourly,2); daily = round(daily,2); monthly = round(monthly,2)
    logCollectorEntry("twominers", owner, 0, "all_miners", hourly, daily, monthly)

# get hourly/daily/monthly income from 2miners wallet
def getEthermineIncome(wallet_address):
    print(f"ETHERMINE: Getting stats for wallet {wallet_address}")
    # Get the account data from 2miners
    url = f"https://api.ethermine.org/miner/{wallet_address}/currentStats"
    requestData = requests.get(url)
    stats = json.loads(requestData.text)['data']
    # Parse the account data
    hourly = stats["usdPerMin"]*60
    daily = hourly*24
    monthly = daily*30
    return (hourly, daily, monthly)

def logEthermineIncome(wallet_address, owner):
    (hourly, daily, monthly) = getEthermineIncome(wallet_address)
    hourly = round(hourly,2); daily = round(daily,2); monthly = round(monthly,2)
    logCollectorEntry("ethermine", owner, 0, "all_miners", hourly, daily, monthly)

def getVastMachines(api_key):
    print(f"VAST: Getting stats for key {api_key}")
    requestData = subprocess.check_output(f"/root/collector/vast --raw --api-key {api_key} show machines".split(" "))
    vastData = json.loads(requestData)
    return vastData

def logVastIncome(api_key, owner):
    stats = getVastMachines(api_key)
    for machine in stats:
        hourly = machine["earn_hour"]
        daily = machine["earn_day"]
        monthly = machine["earn_day"]*30
        spotRentals = machine["gpu_occupancy"].count('I')
        onDemandRentals = machine["gpu_occupancy"].count('D')
        unrented = machine["gpu_occupancy"].count('x')
        logCollectorEntry(
            "vast.ai", owner, machine["id"], machine["hostname"],
            hourly, daily, monthly, spotRentals, onDemandRentals
        )

def getRunpodMachines(api_key):
    print(f"RUNPOD: Getting stats for key {api_key}")
    url = f"https://api.runpod.io/graphql?api_key={api_key}"
    data = { "query": "query { myself { hostBalance machines { id name pods { podType costPerHr desiredStatus volumeInGb containerDiskInGb gpuCount costMultiplier } } } }" }
    requestData = requests.post(url, json=data)
    runpodData = json.loads(requestData.text)["data"]["myself"]["machines"]
    return runpodData

def logRunpodIncome(api_key, owner):
    stats = getRunpodMachines(api_key)
    for machine in stats:
        costPerHr = 0.0
        spotRentals = 0
        onDemandRentals = 0
        for pod in machine["pods"]:
            # CostPerHr
            storageCostPerHr = 0
            if pod["desiredStatus"] == "EXITED":
                storageCostPerHr = float(((pod["volumeInGb"] or 0)*0.2)/30/24)
                costPerHr = costPerHr + storageCostPerHr
            elif pod["desiredStatus"] == "RUNNING":
                storageCostPerHr = float((((pod["volumeInGb"] or 0)+(pod["containerDiskInGb"] or 0))*0.1)/30/24)
                costPerHr = costPerHr + pod["costPerHr"]*pod["costMultiplier"] + storageCostPerHr
            # Rental Count
            if pod["desiredStatus"] == "RUNNING":
                if pod["podType"] == "INTERRUPTABLE": spotRentals += pod["gpuCount"]
                elif pod["podType"] == "RESERVED": onDemandRentals += pod["gpuCount"]
        hourly = costPerHr
        daily = costPerHr*24
        monthly = costPerHr*24*30
        logCollectorEntry(
            "runpod", owner, machine["id"], machine["name"],
            hourly, daily, monthly, spotRentals, onDemandRentals
        )

if __name__ == "__main__":
    try:
        ##### SUNNY
        try:
            print("")
            print("Getting Income Data for Sunny")
            log2MinersIncome("0x035B1148Dc44F8817ED95B5324e5D29fC9C2a79a", "sunny")
            logEthermineIncome("0x035B1148Dc44F8817ED95B5324e5D29fC9C2a79a", "sunny")
            logPoolflareIncome("k:b8601080516e39486aaaa8a5256847ec8462c36f469d958a8a42852466aee4bc", "sunny")
            #logVastIncome("6e074ad4f7f1aaffd98fb90382e5571208c91aab8f1e4a1c4add093a7ad8d020", "sunny")
        except Exception as e:
            print(e)


        ##### MARCHEX
        try:
            print("")
            print("Getting Income Data for Marchex")
            #logEthermineIncome("0xd211a7e80995f0a5e2e164bf9cedf81f22400350", "marchex")
            logEthermineIncome("0x06286edc375e5b9c2c9129056a9013254a6edcae", "marchex")
            logVastIncome("de14f5e6b3607e5b133807ef572d6883f93af9e4e1a5f4d0ff67b54466e93153", "marchex")
            logRunpodIncome("9SVXD9yrmkmZ2P5XGZthxvLVchRqBj", "marchex")
            logCollectorEntry("runpod","marchex","smml16","smml16",2.52,60.41,1812.32,0,8)
        except Exception as e:
            print(e)


        ##### MCI
        try:
            print("")
            print("Getting Income Data for MCI")
            logRunpodIncome("ejZsAK0j8nsn9y5udrBNWdp7JsR9ci", "mci")
            logEthermineIncome("0x433fe2a6634d8acbe8446a4968984dc5d9c8e7c6", "mci")
            ##### Manual entries
            logCollectorEntry("fluid","mci","mcfluid03","mcfluid03",10.42,250.08,7500.00,0,8)
            logCollectorEntry("fluid","mci","mcfluid04","mcfluid04",10.42,250.08,7500.00,0,8)
            logCollectorEntry("fluid","mci","mcfluid05","mcfluid05",6.51,156.24,4687.00,0,5)
            logCollectorEntry("rental","mci","fcdc","fcdc",19.13,459.12,13775.00,0,0)
            logCollectorEntry("runpod","mci","smpod1","smpod1",2.52,60.41,1812.32,0,8)
            logCollectorEntry("qblocks","mci","qblocks01","qblocks01",4.16,99.84,2995.00,0,8)
        except Exception as e:
            print(e)


        ##### NIK
        try:
            print("")
            print("Getting Income Data for Nik")
            #logEthermineIncome("0xe22967d1ed8b11bd970cecb2796184a9c6284787", "nik")
            logRunpodIncome("K7E3GNXD2MUTZGV4N5PME9PQ8FOLBWJ3MCCJ2QQ3", "nik")
        except Exception as e:
            print(e)

    except Exception as e:
        print(e)
    finally:
        if cursor and cursor is not None: cursor.close()
        if cnx and cnx is not None: cnx.close()