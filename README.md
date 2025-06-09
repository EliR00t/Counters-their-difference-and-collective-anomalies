# Counters-their-difference-and-collective-anomalies
Let's start with a wall clock.
![clock](https://github.com/user-attachments/assets/19950c49-6178-4941-8e2b-a870b31d28e5)

9 + 4 = 1 (13 mod 12), the same applies to subtraction 1 - 4 = 9 (-3 mod 12).
We can recall rings from algebra: a set with two defined operations addition and multiplication where some axioms, 
such as commutativity and others, are satisfied. We concentrate here on addition with sign minus which can be thought
as moving in opposite direction on our imaginary clock. We have two modulo to discuss: 
for 32 bit Counters it is 0xFFFFFFFF or 4294967295 and for High Capacity Counters it is 0xFFFFFFFFFFFFFFFF or 18446744073709551615.

My goal is research SNMP Counters using my local network: try to generate some HTTP traffic and possibly ifIn/OutDiscards and ifIn/OutErrors
and try to detect if anomalies in SNMP data.
1. Download and compile and run [static-web-server](https://github.com/static-web-server/static-web-server) and run it using this config:
> cat config.toml
[general]

host = "::"
port = 8080
root = "/home/ilia"

log-level = "info"
log-with-ansi = true

cache-control-headers = true

compression = true
compression-level = "default"

directory-listing = true
directory-listing-order = 1
directory-listing-format = "html"

threads-multiplier = 2

health = true


2. Compile poller and run it: it sends HTTP queries and at the same time polls SNMP counters. It was run under 3 scenarios:
   normal, some distortion was introduced to network interface using **% sudo ts qdisc add dev wlp113s0 root netem delay 100ms loss 5%**
   and some more distortion using **% sudo ts qdisc add dev wlp113s0 root netem delay loss 25% corrupt 5% delay 200ms 50ms**
   Unfortunately this could not make Errors and Discards only SNMP timeouts: network experiments are not easy to design.
3. Data was stored into csv file that can be found under offline analysis: basically it is data exploration and discarding data with low variance, 
   this approach was left us with ifHCInOctets/ifHCOutOctets and ifHCInUcastPkts/ifHCOutUcastPkts. Actually poller calculates rate, i.e. 
   difference in value Counter32/64 divided by duration between measurements (in our case it is expressed in microseconds).
  3a. This task of calculation difference or rate should be done by database but since InfluxDB3 does not support u32, it is done by poller,
      Please add function that takes two u64 converts them to u32 (because these are u32, e.g. ifINError) and calculates difference in u32 semantics.
4. After this we calculate Mahalanobis distance between points and effectively go from 4 dimensional data set to univariate.
5. Collective and point anomalies are decribed in [anomaly package manual](https://cran.r-project.org/web/packages/anomaly/anomaly.pdf)
   and it has a reference to a [paper](https://onlinelibrary.wiley.com/doi/10.1002/sam.11586). In our case it is not just single outlier: 
   polling was performed for a couple of hours while experiencing network packet loss, delay and corruption.
6. My initial intent was to make plugin but it is complicated, so this file not_plugin.py can be run as a cron job.
7. Yes, data transfer between Python and R is a little bit clumsy.




