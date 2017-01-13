################################################################
#This is the parameters file

#There are a number of assumptions and parameters embedded in the productivity analysis,
# this is the place where a lot the assumptions can be changed



###############################################################
###############################################################
###############################################################
###############################################################
###############################################################
###############################################################
###############################################################
###############################################################


#-----------------------------------------------------------------------------
spsgroups <- data.frame(
  rbind(
    data.frame(SPID="SABL",GROUP="SABL"),
    data.frame(SPID="PTRL",GROUP="PTRL"),
    data.frame(SPID="DOVR",GROUP="DOVR"),
    data.frame(SPID="CHLB",GROUP="CHLB"),
    data.frame(SPID=c("ARTH","STRY","UDAB","PDAB","SDAB","BSOL","CSOL",
                      "EGLS","FSOL","SSOL","REX","RSOL"),GROUP=rep("FLAT",12)),
    data.frame(SPID=c("BLCK","BLUR","BRWN","BYEL","CHNA","COPP","CWCD","FLAG","QLBK","SNOS","OLVE","GPHR","GRAS",
                      "BCAC","CNRY","CLPR","WDOW","YTRK","YEYE","POP",
                      "ARRA","BANK","BLGL","DBRK","RCK5","RCK6","REYE","RDBD","USLP","SLRF",
                      "BRNZ","CMEL","DWRF","GBLR","GSRK",
                      "GSPT","HNYC","MXRF","PNKR","PRRK","PGMY",
                      "REDS","RSTN","ROSY","SBLY","SLGR","STLR","STRK","SQRS",
                      "VRML","YLTL","USHR","USLF","RCK1","RCK2","RCK3","RCK4",
                      "RCK7","RCK8","RCK9","URCK"),GROUP=rep("RCK",61)),
    data.frame(SPID=c("LCOD","PCOD","LSPN","SSPN","THDS","THHD"),GROUP=rep("RND",6))
  )
  
)
#--------------------------------------------------------------------------------

#--------------------------------------------------------------------------------
#CATCH-SHARE GROUPS

catchshare.groups <- data.frame(
  rbind(
    data.frame(SPID="ARTH",GROUP="ARTH"),
    data.frame(SPID="BCAC",GROUP="BCAC"),
    data.frame(SPID="CNRY",GROUP="CNRY"),
    data.frame(SPID="CLPR",GROUP="CLPR"),
    data.frame(SPID="CWCD",GROUP="CWCD"),
    data.frame(SPID="DBRK",GROUP="DBRK"),
    data.frame(SPID="DOVR",GROUP="DOVR"),
    data.frame(SPID="EGLS",GROUP="EGLS"),
    data.frame(SPID="LCOD",GROUP="LCOD"),
    data.frame(SPID="LSPN",GROUP="LSPN"),
    data.frame(SPID=c("BRNZ","CMEL","DWRF","GBLR","GSRK",
                      "GSPT","HNYC","MXRF","PNKR","PRRK","PGMY","REDS","RSTN","ROSY","SBLY","SLGR","STLR","STRK","SQRS",
                      "VRML","YLTL","USHR","USLF","RCK1","RCK2","RCK3","RCK4","RCK7","RCK8","RCK9"),GROUP="SHRF"),
    data.frame(SPID=c("ARRA","BANK","BLGL","DBRK","RCK5","RCK6","REYE","RDBD","USLP","SLRF"),GROUP="SLRF"),
    data.frame(SPID=c("UDAB","PDAB","SDAB","BSOL","CSOL",
                      "FSOL","SSOL","REX","RSOL"),GROUP="OFLT"),
    data.frame(SPID="PCOD",GROUP="PCOD"),
    data.frame(SPID="PHAL",GROUP="PHAL"),
    data.frame(SPID="POP",GROUP="POP"),
    data.frame(SPID="PTRL",GROUP="PTRL"),
    data.frame(SPID="SABL",GROUP="SABL"),
    data.frame(SPID="SSPN",GROUP="SSPN"),
    data.frame(SPID="SNOS",GROUP="SNOS"),
    data.frame(SPID="STRY",GROUP="STRY"),
    data.frame(SPID="WDOW",GROUP="WDOW"),
    data.frame(SPID="YEYE",GROUP="YEYE"),
    data.frame(SPID="YTRK",GROUP="YTRK"),
    data.frame(SPID="PWHT",GROUP="PWHT"),
    data.frame(SPID=c("BLCK","BLUR","BRWN","CLCO","COPP","OLVE","QLBK","TREE","BYEL","CBZN","CHNA","GPHR","GRAS"),
               GROUP=rep('NSRF',13))
  )
)

#---------------------------------------------------------------------------------



#---------------------------------------------------------------------------------
#SPECIES GROUPINGS FOR CONSISTENCY WITH MALMQUIST INDEX

sps.groups.m <- data.frame(rbind(
                            data.frame(SPID=c("SABL","DOVR","LSPN","SSPN","THDS","THHD"),GROUP=c(rep("DTS",6))),
                            data.frame(SPID=c("PTRL","CHLB","STRY","UDAB","PDAB","SDAB","BSOL","CSOL",
                                              "EGLS","FSOL","SSOL","REX","RSOL","ARTH"),GROUP=c(rep('FLAT',14))),
                            data.frame(SPID=c("BLCK","BLUR","BRWN","BYEL","CHNA","COPP","CWCD","FLAG","QLBK","SNOS","OLVE",
                                              "GPHR","GRAS",
                                              "BCAC","CNRY","CLPR","WDOW","YTRK","YEYE","POP",
                                              "ARRA","BANK","BLGL","DBRK","RCK5","RCK6","REYE","RDBD","USLP","SLRF",
                                              "BRNZ","CMEL","DWRF","GBLR","GSRK",
                                              "GSPT","HNYC","MXRF","PNKR","PRRK","PGMY",
                                              "REDS","RSTN","ROSY","SBLY","SLGR","STLR","STRK","SQRS",
                                              "VRML","YLTL","USHR","USLF","RCK1","RCK2","RCK3","RCK4",
                                              "RCK7","RCK8","RCK9","URCK"),GROUP=c(rep('RCK',61)))
))

#-------------------------------------------------------------------------------------


###############################################################
###############################################################
###############################################################
###############################################################
###############################################################
###############################################################
###############################################################
###############################################################