let
  pkgs = import <nixpkgs> {};
  rPkgs = with pkgs.rPackages; [ 
    languageserver

    tidyjson jsonlite # json handling, choose one or use csv?
    tidyverse

    httr2 # http(s) requests

    #Database
    dplyr RSQLite DBI

    #UI
    flexdashboard shiny shinydashboard shinyMobile
  ];
in pkgs.mkShell {
  packages = [
    (pkgs.rWrapper.override{packages = rPkgs;})
    (pkgs.rstudioWrapper.override{packages = rPkgs;})
  ];
}
