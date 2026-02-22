//+------------------------------------------------------------------+
//|                                         CalendarSyncService.mq5  |
//|                            Copyright 2024, Trading Robot Service |
//+------------------------------------------------------------------+
// db에 저장되는 시간은 모두 한국 시간으로 변환해서 저장
// db 파일 open/clode는 함수 내부에서 실행하도록 수정 할 것
// 오류 체크 로직 추가 및 출력 코드 보강할 것.

#property service
#property copyright "Copyright 2024"
#property version   "2.00"

// --- 외부 라이브러리 및 설정 ---
#include <_inet.mqh>
#include <JAson.1.13.0.mqh> // CJAVal 클래스 사용을 위한 라이브러리

const string dbFilename = "Shared\\Calendar5.sqlite";
const string arrAlert[] = {60, 30, 15, 5, 0}; // 이벤트 이전 분 단위로 알람 경고
datetime TimeDifference = D'1970.01.01 07:00:00';

//+------------------------------------------------------------------+
//| 서비스 시작 함수 (OnStart)
//+------------------------------------------------------------------+
void OnStart() {

   MqlDateTime local;
   TimeToStruct(TimeLocal(), local);
   // 서머타임에 따른 서버 <-> 한국 시간 보정
   if (IsDST(local))
      TimeDifference = D'1970.01.01 6:00:00';
   else
      TimeDifference = D'1970.01.01 7:00:00';

   // 아래 코드는 db 및 테이블 체크 함수로 대체할 것. 시작
   // 1. DB 열기 (Shared 폴더 내 생성)
   int db = DatabaseOpen(dbFilename, DATABASE_OPEN_CREATE | DATABASE_OPEN_READWRITE);

   if(db == INVALID_HANDLE) {
      Print("DB 열기 실패: ", GetLastError());
      return;
   }

   // 2. 테이블 생성 (hash를 Primary Key로 지정하여 중복 방지)
   string sql_create = "CREATE TABLE IF NOT EXISTS Calendar ("
                       "time INTEGER, event TEXT, code TEXT, hash INTEGER PRIMARY KEY);";
   DatabaseExecute(db, sql_create);

   // 아래 코드는 db 및 테이블 체크 함수로 대체할 것. 끝

   // 프로그램 실행시 db 업데이트
   SyncMT5Calendar(db);
   SyncForexCalendar(db, local);

   // 무한 루프 시작 (주기적 업데이트)
   while(!IsStopped()) {

      Sleep(1000); // 1초 단위 갱신
      TimeToStruct(TimeLocal(), local);

      if (6 == local.day_of_week && 5 < local.hour) continue; // 토요일
      if (0 == local.day_of_week) continue;                   // 일요일
      if (1 == local.day_of_week && 6 > local.hour) continue; // 월요일

      // [주석 보존할 것] 실시간 지표 발표 함수 자리(내장 캘린더 사용)
      // [주석 보존할 것] 인디게이터 디스플레이용 json 파일 출력 함수 자리(db 데이터 사용. 한국시간)

      if (0 != local.sec) continue; // 1분 단위 갱신

      // [주석 보존할 것] 지표 발표 경고 알림 함수 자리(db 데이터 사용. 한국시간)

      if (0 != local.min) continue; // 1시간 단위 갱신

      // MT5 내장 캘린더의 한국과 미국 데이터를 db 저장 (한국시간)
      SyncMT5Calendar(db);
      PrintFormat("[%s] 내장 캘린더 저장 완료", TimeToString(TimeLocal()));

      // 장 운영 정보 db 저장 (한국시간)
      // 장 운영 정보 json 파일 출력 함수 (서버시간)
      PrintFormat("[%s] 장 운영 정보 저장 완료", TimeToString(TimeLocal()));

      if (0 != local.hour % 4) continue; // 4시간 단위 갱신

      // Forex Factory 캘린더 db 저장 (한국시간)
      SyncForexCalendar(db, local);
      PrintFormat("[%s] Forex Factory 캘린더 저장 완료", TimeToString(TimeLocal()));

      if (0 != local.hour % 8) continue; // 8시간 단위 갱신

      // [주석 보존할 것] 데이터 정리 함수 자리
      PrintFormat("[%s] 데이터 정리 완료", TimeToString(TimeLocal()));

      // 서머타임에 따른 서버 <-> 한국 시간 보정
      if (IsDST(local))
         TimeDifference = D'1970.01.01 6:00:00';
      else
         TimeDifference = D'1970.01.01 7:00:00';

      Sleep(66666); // 중복실행 방지를 위한 지연
   }

   DatabaseClose(db);
}

//+------------------------------------------------------------------+
//| [1] MT5 내장 캘린더 동기화 (KR, US)
//+------------------------------------------------------------------+
void SyncMT5Calendar(int db) {
   MqlCalendarValue values[];
   string countries[] = {"KR", "US"};

   // 현재부터 1주일치만 가져옴 (과거 데이터 제외)
   datetime from = TimeTradeServer() - PeriodSeconds(PERIOD_M15);
   datetime to   = TimeTradeServer() + PeriodSeconds(PERIOD_W1);

   for(int i=0; i<ArraySize(countries); i++) {
      if(CalendarValueHistory(values, from, to, countries[i]) > 0) {
         DatabaseExecute(db, "BEGIN TRANSACTION");
         for(int i=0; i<ArraySize(values); i++) {
            MqlCalendarEvent event;
            if(CalendarEventById(values[i].event_id, event)) {
               // 이름 내 줄바꿈 제거
               StringReplace(event.name, "\r", "");
               StringReplace(event.name, "\n", "");

               // 시간 보정 (KST)
               datetime kst_time = values[i].time + TimeDifference;
               string s_kst_time = TimeToString(kst_time, TIME_DATE|TIME_MINUTES);

               // 해시 및 쿼리 실행
               int hash_val = iMakeHash(s_kst_time, (string)event.event_code);
               string sql = StringFormat(
                  "INSERT OR REPLACE INTO Calendar (time, event, code, hash) "
                  "VALUES (%lld, '%s', '%s', %d);",
                  (long)kst_time, event.name, (string)event.event_code, hash_val
               );
               DatabaseExecute(db, sql);
            }
         }
         DatabaseExecute(db, "COMMIT");
      }
   }
}

//+------------------------------------------------------------------+
//| [2] Forex Factory JSON 데이터 동기화 (USD 전용)
//+------------------------------------------------------------------+
bool SyncForexCalendar(int db, MqlDateTime &local) {
   string   url      = "https://nfs.faireconomy.media/ff_calendar_thisweek.json";
   //       url      = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml";
   string   filename = "ff_calendar_thisweek.json";

   // 1시간 주기로 파일 다운로드
   datetime modified = (datetime)FileGetInteger(filename, FILE_MODIFY_DATE, false);
   if(!FileIsExist(filename) || modified < TimeLocal() - 3600) {
      if(!ffUpdate(url, filename)) return false; // 외부 WebRequest 함수
   }

   string data = ReadFullFile(filename); // 파일 읽기 함수
   CJAVal jv;
   if(!jv.Deserialize(data)) return false;

   DatabaseExecute(db, "BEGIN TRANSACTION");
   for(int i = 0; i < jv.Size(); i++) {
      if(jv[i]["country"].ToStr() != "USD") continue;

      datetime event_time = ParseISO8601ToTime(jv[i]["date"].ToStr());
      string event_name = jv[i]["title"].ToStr();

      string event_code = StripString(event_name); // 특수문자 제거 함수
      StringToLower(event_code);

      int hash_val = iMakeHash(TimeToString(event_time, TIME_DATE|TIME_MINUTES), event_code);

      string sql = StringFormat(
         "INSERT OR REPLACE INTO Calendar (time, event, code, hash) "
         "VALUES (%lld, '%s', '%s', %d);",
         (long)event_time, event_name, event_code, hash_val
      );
      DatabaseExecute(db, sql);
   }
   DatabaseExecute(db, "COMMIT");
   return true;
}

bool ffUpdate(string url, string filename) {
   string ff = web_request(url);
   if ("-1" == ff) return (false);

   int handle = FileOpen(filename, FILE_TXT | FILE_WRITE | FILE_SHARE_READ | FILE_ANSI, 0, CP_UTF8);
   if (handle != INVALID_HANDLE) {
      FileWrite(handle, ff);
   }
   FileClose(handle);
   return (true);
}

string ReadFullFile(string filename)
{
   // FILE_ANSI 또는 FILE_UNICODE를 지정하지 않고 CP_UTF8을 조합
   int handle = FileOpen(filename, FILE_TXT | FILE_READ | FILE_SHARE_READ | FILE_ANSI, 0, CP_UTF8);
   if(handle == INVALID_HANDLE) return "";

   string result = "";
   while(!FileIsEnding(handle))
   {
      result += FileReadString(handle);
   }

   FileClose(handle);
   return result;
}

datetime ParseISO8601ToTime(string date_str)
{
   // 1. 기본 날짜 및 시간 부분 추출 (0번째부터 19자: 2024-10-18T12:10:00)
   string base_date = StringSubstr(date_str, 0, 10); // "2024-10-18"
   string base_time = StringSubstr(date_str, 11, 8); // "12:10:00"

   // '-'와 ':'를 '.'과 ':'로 규격화하여 datetime으로 변환
   StringReplace(base_date, "-", ".");
   datetime dt = StringToTime(base_date + " " + base_time);

   // 2. 오프셋 처리 (-04:00 부분 추출)
   if(StringLen(date_str) > 19)
   {
      string sign = StringSubstr(date_str, 19, 1);     // "+" 또는 "-"
      int hours   = (int)StringToInteger(StringSubstr(date_str, 20, 2));
      int minutes = (int)StringToInteger(StringSubstr(date_str, 23, 2));
      int total_offset_seconds = (hours * 3600) + (minutes * 60);

      // 오프셋이 -04:00이면 해당 시간만큼 더해줘야 UTC(표준시)가 됩니다.
      // 반대로 로컬 시간으로 보정하고 싶다면 부호를 반대로 적용하세요.
      if(sign == "+") dt -= total_offset_seconds;
      else if(sign == "-") dt += total_offset_seconds;
   }

   return dt + (9 * 3600); // UTC에서 한국시간으로
}

//+-----------------------------------------------------------------+
//| 날짜가 서머타임 기간인지 검사                                   |
//+-----------------------------------------------------------------+
bool IsDST(const MqlDateTime &dt, bool europe = false) {
   // 1~2월은 전 세계 공통 서머타임 아님
   if (dt.mon < 3 || dt.mon > 11) return false;

   // 4~9월은 전 세계 공통 서머타임 기간
   if (dt.mon > 3 && dt.mon < 10) return true;

   // 해당 날짜가 속한 주의 일요일 날짜 (dt.day_of_week: 0=일, 1=월...)
   int sundayDate = dt.day - dt.day_of_week;

   // --- 유럽(EU) 기준 (3월 마지막 일요일 ~ 10월 마지막 일요일) ---
   if (europe) {
      if (dt.mon == 3)  return (sundayDate >= 25); // 25~31일 사이가 마지막 일요일
      if (dt.mon == 10) return (sundayDate < 25);  // 10월 마지막 일요일 전까지만 true
      return false; // 유럽 11월은 서머타임 아님
   }

   // --- 미국(US) 기준 (3월 2번째 일요일 ~ 11월 1번째 일요일) ---
   if (dt.mon == 3)  return (sundayDate >= 8);  // 8~14일 사이가 2번째 일요일
   if (dt.mon == 11) return (sundayDate < 1);   // 11월 첫 일요일 전까지만 true
   if (dt.mon == 10) return true;               // 미국은 10월 전체가 서머타임

   return false;
}

//+-----------------------------------------------------------------+
//| 알파벳과 숫자만 추출                                            |
//+-----------------------------------------------------------------+
string StripString(string txt, bool special = false) {
   ushort result[]; // 결과를 담을 ushort 배열
   int len = StringLen(txt);
   ArrayResize(result, 0, len); // 메모리 미리 확보

   for (int i = 0; i < len; i++) {
      ushort c = StringGetCharacter(txt, i);

      // 숫자 및 알파벳 체크
      if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
         int size = ArraySize(result);
         ArrayResize(result, size + 1);
         result[size] = c;
      }
      else if (special && (c == '%' || c == '+' || c == '-' || c == '.')) {
         int size = ArraySize(result);
         ArrayResize(result, size + 1);
         result[size] = c;
      }
   }

   // ushort 배열을 한 번에 문자열로 변환 (가장 빠름)
   return ShortArrayToString(result);
}

//+------------------------------------------------------------------+
//| 사용자 정의 해시 함수 (고유 키 생성용)
//+------------------------------------------------------------------+
int iMakeHash(string s1, string s2="", string s3="", string s4="", string s5="") {
   string s = s1 + s2 + s3 + s4 + s5;
   uint hash = 5381;
   for(int i=0; i<StringLen(s); i++) {
      hash = ((hash << 5) + hash) + StringGetCharacter(s, i);
   }
   return (int)(hash & 0x7FFFFFFF);
}

