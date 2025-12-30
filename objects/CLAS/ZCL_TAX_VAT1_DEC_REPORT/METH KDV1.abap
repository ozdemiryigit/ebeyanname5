  METHOD kdv1.

    DATA ls_bkpf  TYPE mty_bkpf.
*    DATA lt_bkpf  TYPE SORTED TABLE OF mty_bkpf WITH UNIQUE KEY bukrs belnr gjahr.
    DATA lt_bkpf  TYPE TABLE OF mty_bkpf .
    DATA lt_bset  TYPE mtty_bset.
    DATA ls_bset  TYPE mty_bset.
    DATA lv_tabix TYPE sy-tabix.

    DATA lv_kbetr_s TYPE p LENGTH 16 DECIMALS 2..
    DATA lv_kbetr_h TYPE p LENGTH 16 DECIMALS 2..
    DATA lv_oran    TYPE p LENGTH 16 DECIMALS 2..
    DATA lv_bypass  TYPE abap_boolean.

    DATA ls_map   TYPE mty_map.
    DATA ls_map2  TYPE mty_map.
    DATA lt_map   TYPE TABLE OF mty_map.
    DATA lv_oran_int TYPE i.
    DATA lv_kiril3 TYPE ztax_e_acklm.

    DATA ls_collect TYPE ztax_ddl_i_vat1_dec_report."mty_collect.
*    DATA ls_account_balances TYPE bapi1028_4.
*    DATA lt_account_balances TYPE TABLE OF bapi1028_4.
    DATA lt_tax_voran TYPE TABLE OF ztax_t_voran.
    DATA ls_tax_voran TYPE  ztax_t_voran.

    TYPES BEGIN OF lty_k1mt.
    TYPES bukrs    TYPE ztax_t_k1mt-bukrs.
    TYPES gjahr    TYPE ztax_t_k1mt-gjahr.
    TYPES monat    TYPE ztax_t_k1mt-monat.
    TYPES kiril1   TYPE ztax_t_k1mt-kiril1.
    TYPES kiril2   TYPE ztax_t_k1mt-kiril2.
    TYPES mwskz    TYPE ztax_t_k1mt-mwskz.
    TYPES kschl    TYPE ztax_t_k1mt-kschl.
    TYPES hkont    TYPE ztax_t_k1mt-hkont.
    TYPES matrah   TYPE ztax_t_k1mt-matrah.
    TYPES vergi    TYPE ztax_t_k1mt-vergi.
    TYPES tevkt    TYPE ztax_t_k1mt-tevkt.
    TYPES manuel   TYPE ztax_t_k1mt-manuel.
    TYPES vergidis TYPE ztax_t_k1mt-vergidis.
    TYPES vergiic  TYPE ztax_t_k1mt-vergiic.
    TYPES END OF lty_k1mt.

    TYPES BEGIN OF lty_topal.
    TYPES split TYPE c LENGTH 10.
    TYPES END OF lty_topal.

    DATA ls_k1mt   TYPE lty_k1mt.
    DATA lt_k1mt   TYPE TABLE OF lty_k1mt.
    DATA ls_topal  TYPE lty_topal.
    DATA lt_topal  TYPE TABLE OF lty_topal.
    DATA lv_kiril2 TYPE ztax_e_kiril2.
    DATA lv_kiril1 TYPE ztax_e_kiril1.

    DATA ls_kostr TYPE mty_kostr.
    DATA lt_kostr TYPE TABLE OF mty_kostr.

    TYPES BEGIN OF lty_kostr_field.
    TYPES split TYPE c LENGTH 10.
    TYPES END OF lty_kostr_field.

    DATA lt_kostr_field TYPE TABLE OF lty_kostr_field.

    DATA lv_index       TYPE i.
    DATA lv_found_index TYPE i.
    DATA lv_length      TYPE i.

    DATA lv_butxt TYPE i_companycode-companycodename.

    TYPES BEGIN OF lty_kschl.
    TYPES sign(1) TYPE c.
    TYPES kschl TYPE kschl.
    TYPES END OF lty_kschl.

    DATA ls_kschl TYPE lty_kschl.
    DATA lt_kschl TYPE TABLE OF lty_kschl.
    DATA lr_kschl TYPE RANGE OF kschl.
    DATA lv_thlog_wrbtr TYPE ztax_t_thlog-wrbtr.

    DATA ls_read_tab TYPE mty_read_tab.
*    DATA lt_bseg TYPE SORTED TABLE OF mty_bseg WITH UNIQUE KEY bukrs belnr gjahr koart. "YiğitcanÖzdemir
    DATA lt_bseg TYPE  TABLE OF mty_bseg .
    DATA ls_bseg TYPE mty_bseg.
    DATA lr_saknr TYPE RANGE OF i_operationalacctgdocitem-operationalglaccount.
    FIELD-SYMBOLS <fs_range>   TYPE any.
    FIELD-SYMBOLS <fs_field>   TYPE any.
    FIELD-SYMBOLS <fs_collect> TYPE ztax_ddl_i_vat1_dec_report." mty_collect.
*    FIELD-SYMBOLS <fs_detail>  TYPE ztax_s_detay_001.
    FIELD-SYMBOLS <lt_outtab> TYPE any .

    DATA : dref_it TYPE REF TO data.
    FIELD-SYMBOLS: <t_outtab>    TYPE any.

    DATA lr_ktosl TYPE RANGE OF ktosl.

    CLEAR me->ms_button_pushed.
    me->ms_button_pushed-kdv1 = abap_true.

    IF iv_bukrs IS NOT INITIAL.
      p_bukrs = iv_bukrs.
    ENDIF.

    IF iv_gjahr IS NOT INITIAL.
      p_gjahr = iv_gjahr.
    ENDIF.

    IF iv_monat IS NOT INITIAL.
      p_monat = iv_monat.
    ENDIF.

    IF iv_beyant IS NOT INITIAL.
      p_beyant = iv_beyant.
    ENDIF.

    IF iv_donemb IS NOT INITIAL.
      p_donemb = iv_donemb.
    ENDIF.


    fill_monat_range( ).
    fill_det_kural_range( ).

    CLEAR mt_collect.
*    CLEAR mt_detail.
*    CLEAR mt_detail_alv.


    me->get_condition_type( IMPORTING et_kostr = lt_kostr ).
    me->get_map_tab( IMPORTING et_map = lt_map ).

    SORT lt_map BY xmlsr ASCENDING.

    me->fill_saknr_range( EXPORTING it_map   = lt_map
                          IMPORTING er_saknr = lr_saknr ).
    me->get_prev_balance( IMPORTING ev_balance = lv_thlog_wrbtr ).

    CLEAR ls_read_tab.
    ls_read_tab-bset = abap_true.
    ls_read_tab-bseg = abap_true.
    me->find_document( EXPORTING is_read_tab = ls_read_tab
                                 ir_saknr    = lr_saknr
                       IMPORTING et_bkpf     = lt_bkpf
                                 et_bset     = lt_bset
                                 et_bseg     = lt_bseg ).

    SELECT bukrs ,
           gjahr ,
           monat ,
           kiril1 ,
           kiril2 ,
           mwskz ,
           kschl ,
           hkont ,
           matrah ,
           vergi ,
           tevkt ,
           manuel ,
           vergidis ,
           vergiic
           FROM ztax_t_k1mt
           WHERE bukrs EQ @p_bukrs
             AND gjahr EQ @p_gjahr
             AND monat IN @mr_monat
            INTO TABLE @lt_k1mt.

    SELECT SINGLE companycodename AS butxt
           FROM i_companycode
           WHERE companycode EQ @p_bukrs
           INTO @lv_butxt.

    SELECT
    j~accountingdocumenttype AS blart,
    j~glaccount AS hkont,
    j~amountincompanycodecurrency  AS tutar
    FROM i_journalentryitem AS j
    INNER JOIN @lt_map AS map
    ON map~saknr = j~glaccount
    AND map~blart = j~accountingdocumenttype
    WHERE j~ledger = '0L'
       AND j~companycode = @p_bukrs
       AND j~fiscalyear = @p_gjahr
       AND j~fiscalperiod = @p_monat
       AND j~isreversal = ''
       AND j~isreversed = ''
       AND j~isreversed = ''
      AND ( j~debitcreditcode = 'S')
      AND map~kiril1 = '30'
    INTO TABLE @DATA(lt_creditcart)  .


    SELECT
    j~accountingdocumenttype AS blart,
    j~glaccount AS hkont,
    j~amountincompanycodecurrency  AS tutar
    FROM i_journalentryitem AS j
    INNER JOIN @lt_map AS map
    ON map~saknr = j~glaccount
    AND map~blart = j~accountingdocumenttype
    WHERE j~ledger = '0L'
       AND j~companycode = @p_bukrs
       AND j~fiscalyear = @p_gjahr
       AND j~fiscalperiod = @p_monat
       AND j~isreversal = ''
       AND ( j~isreversed = 'X' OR j~isreversed = 'X' )
      AND ( j~debitcreditcode = 'H')
      AND map~kiril1 = '30'
    INTO TABLE @DATA(lt_creditcart_rev)  .

    SELECT
    j~glaccount AS hkont,
    SUM(   j~amountincompanycodecurrency   ) AS hwste
      FROM i_journalentryitem AS j
      INNER JOIN @lt_map AS map
      ON map~saknr = j~glaccount
      AND map~kural = '004'
    WHERE j~ledger = '0L'
       AND j~companycode = @p_bukrs
       AND j~fiscalyear = @p_gjahr
       AND j~fiscalperiod = @p_monat
       AND j~isreversal = ''
       AND j~isreversed = ''
       GROUP BY  j~glaccount
    INTO TABLE @DATA(lt_indirim).

    SELECT
    j~glaccount AS hkont,
    j~taxcode AS mwskz,
     r~conditionrateratio AS kbetr,
            SUM( CASE WHEN ( j~transactiontypedetermination = 'VST' OR
                             j~transactiontypedetermination = 'MWS' OR
                             j~transactiontypedetermination = ' ' )  THEN j~amountincompanycodecurrency ELSE 0 END ) AS hwste,
        SUM( CASE WHEN ( j~transactiontypedetermination <> 'VST' AND
                         j~transactiontypedetermination <> 'MWS' AND
                         j~transactiontypedetermination <> 'ZTA' ) THEN j~amountincompanycodecurrency ELSE 0 END ) AS hwbas
      FROM i_journalentryitem AS j
      INNER JOIN @lt_map AS map
      ON map~saknr = j~glaccount
      AND map~mwskz = j~taxcode "eklendi Çağatay-Sümeyye
      AND map~kiril1 = '99'
              LEFT OUTER JOIN i_taxcoderate AS r
        ON r~cndnrecordvaliditystartdate <= j~documentdate
        AND r~cndnrecordvalidityenddate >= j~documentdate
        AND r~taxcode = j~taxcode
    WHERE j~ledger = '0L'
       AND j~companycode = @p_bukrs
       AND j~fiscalyear = @p_gjahr
       AND j~fiscalperiod = @p_monat
       AND j~isreversal = ''
       AND j~isreversed = ''
       GROUP BY  j~glaccount,j~taxcode,r~conditionrateratio
    INTO TABLE @DATA(lt_ozel).
    IF sy-subrc = 0. "eklendi Çağatay - Sümeyye
      LOOP AT lt_ozel ASSIGNING FIELD-SYMBOL(<ls_ozel>). "matrah dogru geliyordu fakat vergi ttuarı yanlıştı o yüzden vergi hesabı yapılıyor.
        <ls_ozel>-hwste = <ls_ozel>-hwbas * ( <ls_ozel>-kbetr / 100 ).
      ENDLOOP.
    ENDIF. "eklendi Çağatay-Sümeyye son



    SELECT
    j~taxcode AS mwskz , r~conditionrateratio AS kbetr ,r~vatconditiontype AS kschl,j~accountingdocumenttype AS blart, j~glaccount AS hkont,
      SUM( CASE WHEN  j~transactiontypedetermination = 'ZTA' THEN j~amountincompanycodecurrency ELSE 0 END ) AS hwste
      FROM i_journalentryitem AS j
      LEFT OUTER JOIN i_taxcoderate AS r
      ON r~cndnrecordvaliditystartdate <= j~documentdate
      AND r~cndnrecordvalidityenddate >= j~documentdate
      AND r~taxcode = j~taxcode
      AND ( r~accountkeyforglaccount = 'VST' OR r~accountkeyforglaccount = 'MWS' )
    WHERE j~ledger = '0L'
       AND j~companycode = @p_bukrs
       AND j~fiscalyear = @p_gjahr
       AND j~fiscalperiod = @p_monat
       AND j~isreversal = ''
       AND j~isreversed = ''
      AND ( j~financialaccounttype = 'S' OR j~financialaccounttype = 'A' )
       AND j~taxcode <> ''
       GROUP BY j~taxcode, r~conditionrateratio,r~vatconditiontype, j~accountingdocumenttype,j~glaccount
    ORDER BY j~taxcode
    INTO TABLE @DATA(lt_109) .


    SORT lt_map BY xmlsr ASCENDING kural ASCENDING.

    LOOP AT lt_map INTO ls_map WHERE topal EQ space.
      CASE ls_map-kural.


        WHEN '001' OR '003' OR '005'.
          CLEAR lv_tabix.

          lr_ktosl = VALUE #( sign = 'I' option = 'EQ' ( low =  'MWS' )
                                                       ( low =  'VST' ) ).
          IF ls_map-saknr IS NOT INITIAL.
            LOOP AT lt_bset INTO ls_bset WHERE mwskz EQ ls_map-mwskz.
*                                           AND hkont EQ ls_map-saknr
*                                           AND ktosl IN lr_ktosl.

              "Hesapçıoğlu için hariç tutuldu.
              IF ls_bset-hkont(3) = '198' OR ls_bset-hkont EQ '6430000001'.
                CONTINUE.
              ENDIF.
              "Hesapçıoğlu için hariç tutuldu.
*

              "1
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.

              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
              "2
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-kiril2 = ls_map-kiril2.
              ls_collect-acklm2 = ls_map-acklm2.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
              ls_collect-islem_tur = ls_map-islem_tur.
              ls_collect-odeme_tur = ls_map-odeme_tur.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
              "3
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-kiril2 = ls_map-kiril2.
              ls_collect-acklm2 = ls_map-acklm2.
              ">>D_ANANTU Comment
              ls_collect-kiril3 = ls_map-mwskz.

              CLEAR lv_oran_int.
*              lv_oran_int = abs( ls_bset-kbetr ) / 10.
              lv_oran_int = abs( ls_bset-kbetr ).
              ls_collect-oran = lv_oran_int.
              SHIFT ls_collect-oran LEFT DELETING LEADING space.
              ls_collect-islem_tur = ls_map-islem_tur.
              ls_collect-odeme_tur = ls_map-odeme_tur.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
            ENDLOOP.
            IF sy-subrc IS NOT INITIAL.
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              COLLECT ls_collect INTO mt_collect.
              ls_collect-kiril2 = ls_map-kiril2.
              ls_collect-acklm2 = ls_map-acklm2.
              COLLECT ls_collect INTO mt_collect.
              ls_collect-kiril3 = ls_map-mwskz.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
            ENDIF.

          ELSE.

            IF ls_map-kiril1 = '109'.

              LOOP AT lt_109 INTO DATA(ls_109) WHERE mwskz = ls_map-mwskz.
*              "1
                CLEAR ls_collect.
                ls_collect-kiril1 = ls_map-kiril1.
                ls_collect-acklm1 = ls_map-acklm1.
*              IF ls_bset-shkzg EQ 'H'.
*
*                ls_collect-matrah = ls_bset-hwbas * -1.
*                ls_collect-vergi  = ls_bset-hwste * -1.
*
*              ELSEIF ls_bset-shkzg EQ 'S'.

*                ls_collect-matrah = ls_109-hwbas .
                ls_collect-vergi  = ls_109-hwste .

*              ENDIF.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
                COLLECT ls_collect INTO mt_collect.
                CLEAR ls_collect.
                "2
                CLEAR ls_collect.
                ls_collect-kiril1 = ls_map-kiril1.
                ls_collect-acklm1 = ls_map-acklm1.
                ls_collect-kiril2 = ls_map-kiril2.
                ls_collect-acklm2 = ls_map-acklm2.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
*                ls_collect-matrah = ls_109-hwbas .
                ls_collect-vergi  = ls_109-hwste .

*              ENDIF.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
                "<<D_ANANTU Alper NANTU Comment
                COLLECT ls_collect INTO mt_collect.
                CLEAR ls_collect.
                "3
                CLEAR ls_collect.
                ls_collect-kiril1 = ls_map-kiril1.
                ls_collect-acklm1 = ls_map-acklm1.
                ls_collect-kiril2 = ls_map-kiril2.
                ls_collect-acklm2 = ls_map-acklm2.
                ">>D_ANANTU Alper NANTU comment
                ls_collect-kiril3 = ls_map-mwskz.

                CLEAR lv_oran_int.
*              lv_oran_int = abs( ls_bset-kbetr ) / 10.
                lv_oran_int = abs( ls_109-kbetr ) .
                ls_collect-oran = lv_oran_int.
                SHIFT ls_collect-oran LEFT DELETING LEADING space.
                ls_collect-vergi  = ls_109-hwste .

*              ENDIF.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
                COLLECT ls_collect INTO mt_collect.
                CLEAR ls_collect.
              ENDLOOP.

            ELSE."109

              LOOP AT lt_bset INTO ls_bset WHERE mwskz EQ ls_map-mwskz.
*                                           AND ktosl IN lr_ktosl.

                "Hesapçıoğlu için hariç tutuldu.
                IF ls_bset-hkont(3) = '198' OR ls_bset-hkont EQ '6430000001'.
                  CONTINUE.
                ENDIF.
                "Hesapçıoğlu için hariç tutuldu.

*
*              "1
                CLEAR ls_collect.
                ls_collect-kiril1 = ls_map-kiril1.
                ls_collect-acklm1 = ls_map-acklm1.
                ls_collect-matrah = ls_bset-hwbas .
                ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
                COLLECT ls_collect INTO mt_collect.
                CLEAR ls_collect.
                "2
                CLEAR ls_collect.
                ls_collect-kiril1 = ls_map-kiril1.
                ls_collect-acklm1 = ls_map-acklm1.
                ls_collect-kiril2 = ls_map-kiril2.
                ls_collect-acklm2 = ls_map-acklm2.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
                ls_collect-matrah = ls_bset-hwbas .
                ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
                "<<D_ANANTU Alper NANTU Comment
                COLLECT ls_collect INTO mt_collect.
                CLEAR ls_collect.
                "3
                CLEAR ls_collect.
                ls_collect-kiril1 = ls_map-kiril1.
                ls_collect-acklm1 = ls_map-acklm1.
                ls_collect-kiril2 = ls_map-kiril2.
                ls_collect-acklm2 = ls_map-acklm2.
                ">>D_ANANTU Alper NANTU comment
                ls_collect-kiril3 = ls_map-mwskz.

                CLEAR lv_oran_int.
*              lv_oran_int = abs( ls_bset-kbetr ) / 10.
                lv_oran_int = abs( ls_bset-kbetr ) .
                ls_collect-oran = lv_oran_int.
                SHIFT ls_collect-oran LEFT DELETING LEADING space.
                ls_collect-matrah = ls_bset-hwbas .
                ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
                ls_collect-islem_tur = ls_map-islem_tur.
                ls_collect-odeme_tur = ls_map-odeme_tur.
                COLLECT ls_collect INTO mt_collect.
                CLEAR ls_collect.
              ENDLOOP.

            ENDIF."109

          ENDIF.
*
        WHEN '004'."Kural 4-Önceki Dönem Hesap Bakiyesi

          READ TABLE lt_indirim INTO DATA(ls_indirim) WHERE hkont EQ ls_map-saknr.
          IF sy-subrc EQ 0.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            ls_collect-vergi  = ls_bset-hwbas.
            COLLECT ls_collect INTO mt_collect.

            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            COLLECT ls_collect INTO mt_collect.
          ELSE.
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            COLLECT ls_collect INTO mt_collect.

            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.

          ENDIF.

        WHEN '010'.

          LOOP AT lt_ozel INTO DATA(ls_ozel) WHERE  hkont = ls_map-saknr
                                               AND  mwskz = ls_map-mwskz.

            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            ls_collect-matrah = ls_ozel-hwbas .
            ls_collect-vergi  = ls_ozel-hwste .

*              ENDIF.
            ls_collect-islem_tur = ls_map-islem_tur.
            ls_collect-odeme_tur = ls_map-odeme_tur.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
            "2
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            ls_collect-matrah = ls_ozel-hwbas .
            ls_collect-vergi  = ls_ozel-hwste .

*              ENDIF.
            ls_collect-islem_tur = ls_map-islem_tur.
            ls_collect-odeme_tur = ls_map-odeme_tur.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
            "3
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            ls_collect-kiril3 = ls_map-mwskz.

            CLEAR lv_oran_int.
            lv_oran_int = abs( ls_ozel-kbetr ) . "eklendi Çağatay - Sümeyye
            ls_collect-oran = lv_oran_int.
            SHIFT ls_collect-oran LEFT DELETING LEADING space.
            ls_collect-matrah = ls_ozel-hwbas .
            ls_collect-vergi  = ls_ozel-hwste .
            ls_collect-islem_tur = ls_map-islem_tur.
            ls_collect-odeme_tur = ls_map-odeme_tur.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
          ENDLOOP.

          IF sy-subrc IS NOT INITIAL.
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril3 = ls_map-mwskz.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
          ENDIF.



        WHEN '011'. "
          CLEAR lv_tabix.
          CLEAR ls_bseg.
          LOOP AT lt_bset INTO ls_bset WHERE  hkont = ls_map-saknr
                                         AND  mwskz = ls_map-mwskz
                                         AND  blart = ls_map-blart
                                         AND  bukrs = p_bukrs
                                         AND  gjahr = p_gjahr.


            "1
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            ls_collect-matrah = ls_bset-hwbas .
            ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
            "2
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            ls_collect-matrah = ls_bset-hwbas .
            ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
            "3
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            ls_collect-kiril3 = ls_map-mwskz.

            CLEAR lv_oran_int.
*            lv_oran_int = abs( ls_bset-kbetr ) / 10.
            lv_oran_int = abs( ls_bset-kbetr ) .
            ls_collect-oran = lv_oran_int.
            SHIFT ls_collect-oran LEFT DELETING LEADING space.
            ls_collect-matrah = ls_bset-hwbas .
            ls_collect-vergi  = ls_bset-hwste .
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
          ENDLOOP.
          IF sy-subrc IS NOT INITIAL.
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril3 = ls_map-mwskz.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
          ENDIF.


        WHEN '012' .
          CLEAR lv_tabix.
          CLEAR ls_bseg.
          IF ls_map-kiril1 = '30'.
            LOOP AT lt_creditcart INTO DATA(ls_credit) WHERE  hkont = ls_map-saknr
                                                           AND  blart = ls_map-blart.

              "1
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-matrah = ls_credit-tutar.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.

            ENDLOOP.
          ELSE.

            LOOP AT lt_bset INTO ls_bset WHERE  hkont = ls_map-saknr
                                                 AND  blart = ls_map-blart.



              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
              "2
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-kiril2 = ls_map-kiril2.
              ls_collect-acklm2 = ls_map-acklm2.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .

*              ENDIF.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
              "3
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-kiril2 = ls_map-kiril2.
              ls_collect-acklm2 = ls_map-acklm2.
              ls_collect-kiril3 = ls_map-mwskz.

              CLEAR lv_oran_int.
*            lv_oran_int = abs( ls_bset-kbetr ) / 10.
              lv_oran_int = abs( ls_bset-kbetr ) .
              ls_collect-oran = lv_oran_int.
              SHIFT ls_collect-oran LEFT DELETING LEADING space.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
            ENDLOOP.

          ENDIF.
          IF sy-subrc IS NOT INITIAL.
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril3 = ls_map-mwskz.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
          ENDIF.


        WHEN '013'.
          CLEAR lv_tabix.
          CLEAR ls_bseg.
          IF ls_map-kiril1 = '99'.
            LOOP AT lt_bset INTO ls_bset WHERE   blart = ls_map-blart.

              "1
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              lv_oran_int = abs( ls_bset-kbetr ) .
              ls_collect-oran = lv_oran_int.
              SHIFT ls_collect-oran LEFT DELETING LEADING space.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .

              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
              "2
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-kiril2 = ls_map-kiril2.
              ls_collect-acklm2 = ls_map-acklm2.
              lv_oran_int = abs( ls_bset-kbetr ) .
              ls_collect-oran = lv_oran_int.
              SHIFT ls_collect-oran LEFT DELETING LEADING space.
              ls_collect-matrah = ls_bset-hwbas .
              ls_collect-vergi  = ls_bset-hwste .
              ls_collect-islem_tur = ls_map-islem_tur.
              ls_collect-odeme_tur = ls_map-odeme_tur.
*              ENDIF.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
              "3
              CLEAR ls_collect.
              ls_collect-kiril1 = ls_map-kiril1.
              ls_collect-acklm1 = ls_map-acklm1.
              ls_collect-kiril2 = ls_map-kiril2.
              ls_collect-acklm2 = ls_map-acklm2.
              ls_collect-kiril3 = ls_map-mwskz.
              ls_collect-islem_tur = ls_map-islem_tur.
              ls_collect-odeme_tur = ls_map-odeme_tur.

              CLEAR lv_oran_int.
*            lv_oran_int = abs( ls_bset-kbetr ) / 10.
              lv_oran_int = abs( ls_bset-kbetr ) .
              ls_collect-oran = lv_oran_int.
              SHIFT ls_collect-oran LEFT DELETING LEADING space.
              ls_collect-matrah = ls_credit-tutar * -1.
              COLLECT ls_collect INTO mt_collect.
              CLEAR ls_collect.
            ENDLOOP.

          ENDIF.
          IF sy-subrc IS NOT INITIAL.
            CLEAR ls_collect.
            ls_collect-kiril1 = ls_map-kiril1.
            ls_collect-acklm1 = ls_map-acklm1.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril2 = ls_map-kiril2.
            ls_collect-acklm2 = ls_map-acklm2.
            COLLECT ls_collect INTO mt_collect.
            ls_collect-kiril3 = ls_map-mwskz.
            COLLECT ls_collect INTO mt_collect.
            CLEAR ls_collect.
          ENDIF.


      ENDCASE.
    ENDLOOP.





    LOOP AT mt_collect ASSIGNING <fs_collect>.

      <fs_collect>-matrah   = abs( <fs_collect>-matrah ).
      <fs_collect>-vergi    = abs( <fs_collect>-vergi ).
      <fs_collect>-tevkifat = abs( <fs_collect>-tevkifat ).

    ENDLOOP.


    " manuel olanları toplayalım .

    CLEAR ls_map.
    SORT lt_map BY kiril1 kiril2.
    LOOP AT lt_k1mt INTO ls_k1mt.

      CLEAR ls_map.
      READ TABLE lt_map INTO ls_map WITH KEY kiril1 = ls_k1mt-kiril1
                                             kiril2 = ls_k1mt-kiril2
                                             BINARY SEARCH.

      READ TABLE mt_collect ASSIGNING <fs_collect> WITH KEY kiril1 = ls_k1mt-kiril1
                                                            kiril2 = space.
      IF <fs_collect> IS ASSIGNED.
        <fs_collect>-matrah = <fs_collect>-matrah + ls_k1mt-matrah.
        <fs_collect>-vergi = <fs_collect>-vergi + ls_k1mt-vergi.
        <fs_collect>-vergi = <fs_collect>-vergi + ls_k1mt-tevkt.
*        ADD ls_k1mt-matrah TO <fs_collect>-matrah.
*        ADD ls_k1mt-vergi  TO <fs_collect>-vergi.
*        ADD ls_k1mt-tevkt  TO <fs_collect>-vergi.
        UNASSIGN <fs_collect>.
      ENDIF.

      READ TABLE mt_collect ASSIGNING <fs_collect> WITH KEY kiril1 = ls_k1mt-kiril1
                                                            kiril2 = ls_k1mt-kiril2.
      IF <fs_collect> IS ASSIGNED.
        <fs_collect>-matrah = <fs_collect>-matrah + ls_k1mt-matrah.
        <fs_collect>-vergi = <fs_collect>-vergi + ls_k1mt-vergi.
        <fs_collect>-vergi = <fs_collect>-vergi + ls_k1mt-tevkt.
*        ADD ls_k1mt-matrah TO <fs_collect>-matrah.
*        ADD ls_k1mt-vergi  TO <fs_collect>-vergi.
*        ADD ls_k1mt-tevkt  TO <fs_collect>-vergi.
        UNASSIGN <fs_collect>.
      ENDIF.

      READ TABLE mt_collect ASSIGNING <fs_collect> WITH KEY kiril1 = ls_k1mt-kiril1
                                                            kiril2 = ls_k1mt-kiril2
                                                            kiril3 = ls_k1mt-mwskz.
      IF <fs_collect> IS ASSIGNED.
        <fs_collect>-matrah = <fs_collect>-matrah + ls_k1mt-matrah.
        <fs_collect>-vergi = <fs_collect>-vergi + ls_k1mt-vergi.
        <fs_collect>-vergi = <fs_collect>-vergi + ls_k1mt-tevkt.
*        ADD ls_k1mt-matrah TO <fs_collect>-matrah.
*        ADD ls_k1mt-vergi  TO <fs_collect>-vergi.
*        ADD ls_k1mt-tevkt  TO <fs_collect>-vergi.
        UNASSIGN <fs_collect>.
      ENDIF.


    ENDLOOP.
    "<
    LOOP AT lt_map INTO ls_map WHERE topal NE space.

      CONDENSE ls_map-topal NO-GAPS.
      CLEAR lt_topal.
      CLEAR lv_kiril2.
      CLEAR lv_kiril1.
      SPLIT ls_map-topal AT '+' INTO TABLE lt_topal.

      LOOP AT lt_topal INTO ls_topal.
        CLEAR lv_kiril2.
        CLEAR ls_map2.
        SHIFT ls_topal-split LEFT DELETING LEADING space.
        lv_kiril1 = ls_topal-split.

        LOOP AT mt_collect INTO ls_collect WHERE kiril1 EQ lv_kiril1
                                             AND kiril2 EQ lv_kiril2
                                             AND kiril3 EQ space.
          CLEAR : ls_collect-islem_tur.
          ls_collect-kiril1 = ls_map-kiril1.
          ls_collect-acklm1 = ls_map-acklm1.
          ls_collect-kiril2 = ls_map-kiril2.
          ls_collect-acklm2 = ls_map-acklm2.


          IF ls_map-topalk EQ '001'.
            CLEAR ls_collect-matrah.
            CLEAR ls_collect-oran.
            CLEAR ls_collect-tevkifat.
            CLEAR ls_collect-tevkifato.
          ELSE.
            CLEAR ls_collect-vergi.
            CLEAR ls_collect-tevkifat.
          ENDIF.

          COLLECT ls_collect INTO mt_collect.
          CLEAR ls_collect.

        ENDLOOP.
      ENDLOOP.
      IF ls_map-kural EQ '007'.
        READ TABLE mt_collect ASSIGNING <fs_collect> WITH KEY kiril1 = ls_map-kiril1
                                                              kiril2 = ls_map-kiril2.
        IF sy-subrc IS INITIAL.
          <fs_collect>-matrah = <fs_collect>-matrah + lv_thlog_wrbtr.
*          ADD lv_thlog_wrbtr TO <fs_collect>-matrah.
          UNASSIGN <fs_collect>.
        ENDIF.
      ENDIF.
    ENDLOOP.

    et_collect = mt_collect.
    er_monat   = mr_monat.

  ENDMETHOD.