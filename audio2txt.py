import warnings
import speech_recognition as sr
# pip install PyAudio-0.2.11-cp37-cp37m-win32.whl for windows 10
import pyaudio
#from gtts import gTTS

#def txt2mp3(msg,fn):    
#    myobj = gTTS(text=msg, lang="zh-CN", slow=False)
#    myobj.save(fn)
#    print("you need to do this : ffmpeg -y -i sample.mp3 sample.wav")
#    return

def wav2txt_auto(fn):    
    pass_rate = 0.8
    best_score = pass_rate
    lang_detected = 'en'
    transcript = ""
    lang_list = ['en', 'en-UK','en-US','zh-CN','zh-TW', 'zh-YUE','hi-IN','ta-Sg','bn-BD','fil-PH','id-ID','ms-MY','my-MM','th-TH','vi-VN','ja-JP','ko-KR','nl-NL','fr-FR','de-DE','it-IT','es-ES']
    try:
        r = sr.Recognizer()
        with sr.AudioFile(fn) as src:
            audio = r.record(src)
            for lang in lang_audio:
                score = 0
                try:
                    result = r.recognize_google(audio,language=lang, show_all=True)                    
                except:
                    pass
                if 'alternative' in list(result):
                    txt = result['alternative'][0]['transcript']
                    score =  result['alternative'][0]['confidence']
                    if score >= pass_rate and score > best_score and txt != "":
                        best_score = score
                        lang_detected = lang
                        transcript = txt
                        #print(f"lang = {lang} score = {score} transcript = {txt}")
                        # {'alternative': [{'transcript': '你好吗现在休息一下', 'confidence': 0.97500241}], 'final': True}
                        #break
    except:
        pass    
    return (lang_detected, transcript, best_score)

txt = ""
(lang, txt, score) = wav2txt_auto("sample.wav")
print("="*50)
print(f"Language found {lang}, transcript = {txt}, accuracy = {score}")
