#
# __     __ __       __ __       __    __ __    __ __       ______ _______  
#|  \   |  \  \     /  \  \     |  \  /  \  \  |  \  \     |      \       \ 
#| ▓▓   | ▓▓ ▓▓\   /  ▓▓ ▓▓     | ▓▓ /  ▓▓ ▓▓  | ▓▓ ▓▓      \▓▓▓▓▓▓ ▓▓▓▓▓▓▓\
#| ▓▓   | ▓▓ ▓▓▓\ /  ▓▓▓ ▓▓     | ▓▓/  ▓▓| ▓▓__| ▓▓ ▓▓       | ▓▓ | ▓▓__/ ▓▓
# \▓▓\ /  ▓▓ ▓▓▓▓\  ▓▓▓▓ ▓▓     | ▓▓  ▓▓ | ▓▓    ▓▓ ▓▓       | ▓▓ | ▓▓    ▓▓
#  \▓▓\  ▓▓| ▓▓\▓▓ ▓▓ ▓▓ ▓▓     | ▓▓▓▓▓\ | ▓▓▓▓▓▓▓▓ ▓▓       | ▓▓ | ▓▓▓▓▓▓▓\
#   \▓▓ ▓▓ | ▓▓ \▓▓▓| ▓▓ ▓▓_____| ▓▓ \▓▓\| ▓▓  | ▓▓ ▓▓_____ _| ▓▓_| ▓▓__/ ▓▓
#    \▓▓▓  | ▓▓  \▓ | ▓▓ ▓▓     \ ▓▓  \▓▓\ ▓▓  | ▓▓ ▓▓     \   ▓▓ \ ▓▓    ▓▓
#     \▓    \▓▓      \▓▓\▓▓▓▓▓▓▓▓\▓▓   \▓▓\▓▓   \▓▓\▓▓▓▓▓▓▓▓\▓▓▓▓▓▓\▓▓▓▓▓▓▓ 
#
# Library functions by KH
#------------------------------------------------------------------------------------------------------
summary = """
╔«═══════════════════════════════════════════════════════════════════════•[^]»╗
║ ███████████████████████     Functions Name       ███████████████████████    ║▒▒
╟─────────────────────────────────────────────────────────────────────────────╢▒▒
║ convert_audio      convert audio file to a specific format using ffmpeg     ║▒▒
║ image_detect       image detection based on a given trained model           ║▒▒
║ process_voice      convert audio/video into .wav audio before wav2txt       ║▒▒
║ readtxt_image      text recognition from a picture or image document        ║▒▒
║ readtxt_pdf        text recognition from pdf (limited support)              ║▒▒
║ text2voice         convert text to audo using google gTTS api               ║▒▒
║ video_detect       video image detection with a give nmodel
║ video_training     train a model using video image                          ║▒▒
║ wav2txt            text recognition using google speech_recognition api     ║▒▒
╟─────────────────────────────────────────────────────────────────────────────╢▒▒
║ python3.7 -m pip install opencv-contrib-python --upgrade                    ║▒▒
╚═════════════════════════════════════════════════════════════════════════════╝▒▒
 ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
"""

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

from PIL import Image
import pdftotext               
import cv2                     
import pytesseract             
import gtts                    
from gtts import gTTS          
import pyaudio
import speech_recognition as sr
import numpy as np
import os

from vmsvclib import *

def convert_audio(fname, fmt = ".wav"):
    try:
        fn = fname.split('.')[0]
        fn += "." + fmt
        cmd = f"ffmpeg -y -i {fname} {fn}"
        txt = shellcmd(cmd)
    except:
        fn = ""
    return fn

def image_detect(fname, trainer='trainer.yml'):
    recognizer = cv2.face.LBPHFaceRecognizer_create()
    recognizer.read(trainer)
    cascadePath = "haarcascade_frontalface_default.xml"
    detector = cv2.CascadeClassifier(cascadePath);
    font = cv2.FONT_HERSHEY_SIMPLEX
    PIL_img = Image.open(fname).convert('L')
    img_numpy = np.array(PIL_img,'uint8')
    faces = detector.detectMultiScale(img_numpy)
    if len(faces)==0:
        return (0, 0)
    (x,y,w,h) = faces[0]
    cv2.rectangle(img_numpy, (x,y), (x+w,y+h), (0,255,0), 2)
    id, confidence = recognizer.predict(img_numpy[y:y+h,x:x+w])
    cv2.destroyAllWindows()
    return (id, confidence)

def process_voice(fname, lang="en"):
    try:
        txt = ""
        if '.wav' in fname:
            wav = fname
        else:
            wav = convert_audio(fname, "wav")
        if wav != "":
            (lang_detected, txt, best_score) = wav2txt(wav, lang)
            os.remove(wav)
        if wav != fname:
            os.remove(fname)
    except:
        pass
    return txt

def readtxt_image(fn):
    txt = ""
    try:
        img = cv2.imread(fn)
        txt = pytesseract.image_to_string(img)
    except:
        txt = "Thanks for the image but I am not able read it"        
    return txt

def readtxt_pdf(fn):
    # if hosted in pythonanywhere, system call /usr/bin/pdftotext directly instead of using this
    txt = ""
    try:
        with open(fn, "rb") as f:
            pdf = pdftotext.PDF(f)
        txt = "".join(pdf)
    except:
        if os.name == "nt":
            cmd = "pdftotext.exe "
        else:
            cmd = "/usr/bin/pdftotext "
        cmd += fn + " result.txt"
        txt = shellcmd(cmd)
        f = open("result.txt", "r")
        txt = f.read()
        f.close()
        os.remove("result.txt")
    return txt

def text2voice(bot, chat_id, lang, resp):
    try:
        mp3 = 'echobot' + str(chat_id) + '.mp3'
        myobj = gTTS(text=resp, lang=lang, slow=False)
        myobj.save(mp3)
        fn = convert_audio(mp3, "ogg")
        if fn != "":
            bot.sendAudio(chat_id, (fn, open(fn, 'rb')), title='text to voice')
            os.remove(fn)
        os.remove(mp3)
    except:
        pass
    return

def video_detect(fname, trainer="trainer.yml"):
    recognizer = cv2.face.LBPHFaceRecognizer_create()
    recognizer.read(trainer)
    cascadePath = "haarcascade_frontalface_default.xml"
    faceCascade = cv2.CascadeClassifier(cascadePath);
    font = cv2.FONT_HERSHEY_SIMPLEX
    cam = cv2.VideoCapture(fname)
    cam.set(3, 640) # set video widht
    cam.set(4, 480) # set video height
    minW = 0.1*cam.get(3)
    minH = 0.1*cam.get(4)
    Face_is_unknown = True
    id = "unknown"
    while Face_is_unknown:
        ret, img =cam.read()
        try:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            faces = faceCascade.detectMultiScale(gray,scaleFactor = 1.2,minNeighbors = 5,minSize = (int(minW), int(minH)),)
        except:
            break
        rate = 0
        for(x,y,w,h) in faces:
            cv2.rectangle(img, (x,y), (x+w,y+h), (0,255,0), 2)
            id, confidence = recognizer.predict(gray[y:y+h,x:x+w])
            rate = rate if rate>confidence else confidence
    cam.release()
    cv2.destroyAllWindows()
    return id, rate

def video_training(fn, userid):
    cam = cv2.VideoCapture(fn) 
    recognizer = cv2.face.LBPHFaceRecognizer_create()
    face_detector = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
    currentframe = 0
    face_id = str(userid)
    faceSamples=[]
    ids = []
    id = userid
    loop_again = True
    while loop_again : 
        ret,frame = cam.read() 
        try:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        except:
            loop_again = False
        faces = face_detector.detectMultiScale(gray, 1.3, 5)
        for (x,y,w,h) in faces:
            cv2.rectangle(frame, (x,y), (x+w,y+h), (255,0,0), 2)
            if ret: 
                img_numpy = np.array(gray[y:y+h,x:x+w],'uint8')
                faceSamples.append( gray[y:y+h,x:x+w] )
                ids.append(id)
                currentframe += 1
            else: 
                break
    cam.release() 
    cv2.destroyAllWindows() 
    recognizer.train(faceSamples, np.array(ids))
    recognizer.write('trainer.yml') 
    return

def wav2txt(wavfile, lang="en-US"):
    pass_rate = 0.8
    best_score = pass_rate
    lang_detected = 'en'
    transcript = ""    
    if lang=="auto":
        lang_list = ['en', 'en-UK','en-US','zh-CN','zh-TW', 'zh-YUE','hi-IN','ta-Sg','bn-BD','fil-PH','id-ID','ms-MY','my-MM','th-TH','vi-VN','ja-JP','ko-KR','nl-NL','fr-FR','de-DE','it-IT','es-ES']
    else:
        lang_list = [lang]
    try:
        r = sr.Recognizer()
        if isinstance(r, sr.Recognizer):
            wav = sr.AudioFile(wavfile)
            with wav as source:
                audio = r.record(source)
            for vlang in lang_list:
                score = 0
                txt = ""
                result = r.recognize_google(audio,language=vlang, show_all=True)                    
                if 'alternative' in list(result):
                    txt = result['alternative'][0]['transcript']
                    score =  result['alternative'][0]['confidence']
                    if score >= pass_rate and score > best_score and txt != "":
                        best_score = score
                        lang_detected = vlang
                        transcript = txt
    except:
        print("Error using Recognizer")
        pass
    return (lang_detected, transcript, best_score)

if __name__ == "__main__":
    #encrypt_email("FOS-1219A.db")
    print("This is vmlkhlib")
