function red(s) {
    printf "\033[1;31m" s "\033[0m "
}

function green(s) {
    printf "\033[1;42m" s "\033[0m "
}

function yellow(s) {
    printf "\033[1;43m" s "\033[0m "
}

$2==$3 {printf $0}
$2!=$3 {
    if($2=="NaN")
    {
        green($0);
        print $0 > "tmp";
        n+=1;
    }
    else if($3=="NaN")
        yellow($0);
    else
        red($0);
}

{printf "\n"}
END{print n}
