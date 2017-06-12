
# insert emq-sn into Makefile
f = open("emq-relx/Makefile", "r")
data = f.read()
f.close()
if data.find("emq_sn") < 0:
    f = open("emq-relx/Makefile", "w")
    data = data.replace("emq_auth_pgsql emq_auth_mongo", "emq_auth_pgsql emq_auth_mongo emq_sn")
    data = data.replace("dep_emq_auth_mongo", "dep_emq_sn = git https://github.com/emqtt/emq-sn.git develop\ndep_emq_auth_mongo")
    f.write(data)
    f.close()


f = open("emq-relx/relx.config", "r")
data = f.read()
f.close()
if data.find("emq_sn") < 0:
    f = open("emq-relx/relx.config", "w")
    data = data.replace("{emq_dashboard, load},", "{emq_dashboard, load},\n{emq_sn, load},")
    f.write(data)
    f.close()

