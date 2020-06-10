// FEEL FREE TO ADD STUFF BUT DO NOT REMOVE ANYTHING

exception IllegalArgument {
  1: string message;
}

service BcryptService {
 void initializeBackend (1: string host, 2: i32 port) throws (1: IllegalArgument e);
 list<string> hashPassword (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPassword (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
 list<string> hashPasswordBE (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPasswordBE (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
}
