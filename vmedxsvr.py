#------------------------------------------------------------------------------------------------------#
# / _ \ _ __ ___  _ __ (_)  \/  | ___ _ __ | |_ ___  _ __
#| | | | '_ ` _ \| '_ \| | |\/| |/ _ \ '_ \| __/ _ \| '__|
#| |_| | | | | | | | | | | |  | |  __/ | | | || (_) | |
# \___/|_| |_| |_|_| |_|_|_|  |_|\___|_| |_|\__\___/|_|
#
# EDX Server                                            
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore")
import sys
import json
import asyncio
import vmedxlib
import vmsvclib
global tcp_port,loop

async def handle_echo(reader, writer):
    global loop
    data = await reader.read(100)
    message = data.decode()    
    addr = writer.get_extra_info('peername')
    if message=="/stopedx":
        loop.stop()
        return
    arg_list = message.split("|")
    arglen=len(arg_list)
    if arglen>0:
        client_name = arg_list[0]
        cmd = arg_list[1] if arglen>1 else ""
        resp = arg_list[2] if arglen>2 else ""
        arg = arg_list[3] if arglen>3 else ""
        cmd = cmd.replace("update_","")
        print(client_name,cmd,resp,arg)
        vmedxlib.parse_commands(client_name,cmd,resp,arg)
        message=str(arg_list)
    else:
        print(message)
    data=message.encode()
    writer.write(data)
    await writer.drain()
    writer.close()
    return 

async def chat(msg):
    global tcp_port
    try:
        reader, writer = await asyncio.open_connection('localhost',tcp_port)
        writer.write(msg.encode())
        data = await reader.read(100)
        txt = f'Received: {data.decode()!r}'
        print(txt)
        writer.close()
    except:
        return

async def streams_server():
    global tcp_port
    server = await asyncio.start_server(
        handle_echo, 'localhost', tcp_port)
    addr = server.sockets[0].getsockname()    
    async with server:
        await server.serve_forever()    
    return 

if __name__ == "__main__":
    global use_edxapi,loop,tcp_port
    with open("vmbot.json") as json_file:
        bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    tcp_port = int(bot_info['tcp_port']) if 'tcp_port' in list(bot_info) else 0
    if tcp_port==0:
        print("Please define tcp_port in the vmbot.json.\nTo disable the EDX server, set tcp_port to 0")
        sys.exit(0)
    vmedxlib.edx_api_url = "https://omnimentor.sambaash.com/edx/v1"
    vmedxlib.edx_api_header = {'Authorization': 'Basic ZWR4YXBpOlVzM3VhRUxJVXZENUU4azNXdG9E', 'Content-Type': 'text/plain'}
    vmsvclib.rds_connstr = bot_info['omdb']
    vmsvclib.rdsdb = None
    vmsvclib.rdscon = None
    vmsvclib.rds_pool = 0
    vmsvclib.rds_schema = bot_info['schema']
    vmedxlib.max_iu = 20
    cmd = str(sys.argv[1]) if len(sys.argv)>=2 else ""
    resp = str(sys.argv[2]) if len(sys.argv)>=3 else ""
    arg = str(sys.argv[3]) if len(sys.argv)>=4 else ""
    if cmd=="/startedx":
        print(f"EDX server via port {tcp_port}")
        try:
            loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)
            task = loop.create_task(streams_server())
            loop.run_forever()
        except KeyboardInterrupt:
            task.cancel()
            txt='Thank you for using OmniMentor EDX Server. Goodbye!'
            print(txt)
        finally:
            pass
    elif cmd=="/testedx":
        func_req = "info"
        course_id = "course-v1:Lithan+FOS-0121A+11jan2021"
        nl="|"
        message=f"{client_name}{nl}{func_req}{nl}{course_id}"
        asyncio.run(chat(message))
    elif cmd=="/stopedx":
        asyncio.run(chat(cmd))
    else:
        prog = str(sys.argv[0])
        print(f"usage :\n\tpython3 {prog} [commands]")
        print("commands:\n\t/testedx\n\t/startedx\n\t/stopedx\n")
        print(f"Example:\n\tpython3 {prog} /testedx")
