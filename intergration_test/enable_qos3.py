
input = open("emq-relx/deps/emq_sn/etc/emq_sn.conf")
lines = input.readlines()
input.close()

output  = open("emq-relx/deps/emq_sn/etc/emq_sn.conf",'w');
for line in lines:
    if not line:
        break
    if 'enable_qos3' in line:
        line = line.replace('off','on')
        output.write(line)
    else:
        output.write(line)
output.close()


