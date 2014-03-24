include ../bin/common.defs

################# Users Change This ##################

MODULE = ads

##############################

ifeq ($(OMAKE_VERBOSE),1)
ANT_VERBOSE=-verbose
else
ANT_VERBOSE=-quiet
endif

clean::
	cmd /c call ${ANT} ${ANT_VERBOSE} clean
	${RM} dependencies

preserve:
	cmd /c call ${ANT} ${ANT_VERBOSE} compile preserve

test: 

testAll: test
	
killNodes:
 
include ${WORK}/bin/common.rules
