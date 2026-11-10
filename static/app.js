'use strict';
const file = document.querySelector('#file'), choose = document.querySelector('#choose'), list = document.querySelector('#list'), message = document.querySelector('#message'), drop = document.querySelector('#drop');
let selected = null, state = null, sending = false, timer = null;
let lastList = '';
const labels = {processing:'Обрабатывается', ready:'Готово', error:'Ошибка'};
function node(tag, text, cls) { const el = document.createElement(tag); if(text !== undefined) el.textContent = text; if(cls) el.className = cls; return el; }
function notice(text, bad=false) { message.hidden = !text; message.textContent=text; message.className='message'+(bad?' bad':''); }
async function api(url, options) { const response=await fetch(url, options); let body; try { body=await response.json(); } catch {throw new Error('Не удалось связаться с сервером. Попробуйте позже');} if(!response.ok) throw new Error(body.message || 'Не удалось выполнить запрос'); return body; }
function limits() {
  if(sending) return;
  choose.disabled = !state || !state.available;
  choose.textContent = 'Выбрать запись ↗';
  if(!state) return;
  if(state.total_exhausted) notice('Общий лимит приложения — 20 обработок — исчерпан. Новые записи больше не принимаются.',true);
  else if(state.daily_exhausted) notice('Сегодня вы уже использовали обработку. Следующая попытка доступна после 00:00 по московскому времени.');
  else if(state.busy) notice('Обрабатывается запись. Дождитесь завершения; результат появится в истории.');
  else notice('');
}
async function detail(id, container) {
  container.replaceChildren(node('p','Загружаем результат…','muted'));
  try {
    const data=await api('/api/meetings/'+encodeURIComponent(id)); container.replaceChildren();
    if(data.status==='processing') {container.append(node('p','Обрабатывается. Результат появится здесь автоматически.')); return;}
    if(data.status==='error') {
      container.append(node('p',data.error_message || 'Не удалось обработать запись'));
      if(data.attempt_consumed_at) container.append(node('p','Сегодняшняя попытка использована.','muted'));
      container.append(node('p','Можно выбрать файл повторно с учётом суточного и общего лимитов.','muted'));
      if(state?.available) {const retry=node('button','Выбрать файл повторно','retry'); retry.type='button'; retry.onclick=()=>file.click();container.append(retry);} return;
    }
    const s=data.summary;
    container.append(node('h3','Краткое содержание'),node('p',s.summary));
    for(const [title, items, empty] of [['Решения',s.decisions,'Решения не зафиксированы'],['Задачи',s.tasks,'Задачи не зафиксированы'],['Открытые вопросы',s.open_questions,'Открытые вопросы не зафиксированы']]) {
      container.append(node('h3',title));
      if(!items.length) container.append(node('p',empty));
      else {const ul=node('ul'); for(const item of items) {const li=node('li',typeof item==='string'?item:item.text); if(typeof item!=='string') li.append(node('div',`Исполнитель: ${item.assignee || 'Не указан'} · Срок: ${item.deadline || 'Не указан'}`,'muted')); ul.append(li);} container.append(ul);}
    }
    const transcript=node('details',undefined,'transcript');transcript.append(node('summary','Расшифровка'),node('p',data.transcript));container.append(transcript);
  } catch(error) {container.replaceChildren(node('p',error.message));}
}
async function refresh() {
  clearTimeout(timer);
  try {
    const [newState, meetings]=await Promise.all([api('/api/session'),api('/api/meetings')]); state=newState; limits();
    const signature=JSON.stringify(meetings);
    if(signature===lastList) {timer=setTimeout(refresh,state.busy?3000:30000);return;}
    lastList=signature;
    const open = new Set([...list.querySelectorAll('details.meeting[open]')].map(el=>el.dataset.id));
    list.replaceChildren();
    if(!meetings.length) list.append(node('p','Здесь появятся ваши встречи. Начните с первой записи.','empty'));
    for(const m of meetings) {
      const el=node('details',undefined,'meeting');el.dataset.id=m.id;
      const summary=node('summary'), name=node('div');name.append(node('span',m.filename,'meeting-title'),node('span',new Date(m.created_at).toLocaleString('ru-RU',{timeZone:'Europe/Moscow',dateStyle:'medium',timeStyle:'short'})+' · МСК','meeting-date'));
      summary.append(name,node('span',labels[m.status], 'status '+m.status));const result=node('div',undefined,'result');el.append(summary,result);
      el.addEventListener('toggle',()=>{if(el.open) {selected=m.id;detail(m.id,result);}});
      list.append(el); if(open.has(m.id)||selected===m.id) el.open=true;
    }
    timer=setTimeout(refresh, state.busy || meetings.some(m=>m.status==='processing') ? 3000 : 30000);
  } catch(error) {notice(error.message,true);choose.disabled=true;timer=setTimeout(refresh,10000);}
}
async function sendFile(value) {
  if(sending || !value) return;
  if(!state?.available) {limits();return;}
  if(!/\.(mp4|webm|mp3|wav|m4a)$/i.test(value.name)) {notice('Выберите файл MP4, WebM, MP3, WAV или M4A',true);return;}
  if(!value.size || value.size>104857600) {notice(value.size?'Файл превышает допустимый размер 100 МиБ':'Выбран пустой файл',true);return;}
  sending=true;choose.disabled=true;choose.textContent='Загрузка файла…';notice('Загрузка файла…');clearTimeout(timer);
  const form=new FormData();form.append('file',value);
  let failure='';
  try {const result=await api('/api/meetings',{method:'POST',body:form});selected=result.id;} catch(error){failure=error.message;}
  finally {sending=false;file.value='';await refresh();if(failure)notice(failure,true);}
}
choose.addEventListener('click',()=>file.click());file.addEventListener('change',()=>sendFile(file.files[0]));
for(const event of ['dragenter','dragover']) drop.addEventListener(event,e=>{e.preventDefault();drop.classList.add('over');});
for(const event of ['dragleave','drop']) drop.addEventListener(event,e=>{e.preventDefault();drop.classList.remove('over');});
drop.addEventListener('drop',e=>{if(e.dataTransfer.files.length!==1)notice('Выберите один файл записи',true);else sendFile(e.dataTransfer.files[0]);});
refresh();
