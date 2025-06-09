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
1. Download and compile and run [static-web-server](https://github.com/static-web-server/static-web-server)

