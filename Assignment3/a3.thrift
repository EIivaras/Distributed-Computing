service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void connect(1: string host, 2: i32 port);
  void replicateData(1: map<string,string> dataMap);
  void replicatePut(1: string key, 2: string value);
}
